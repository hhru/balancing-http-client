import asyncio
import logging
import time

from collections import OrderedDict
from random import random, shuffle
from tornado.ioloop import IOLoop
from tornado.httpclient import HTTPRequest, HTTPResponse, HTTPError
from tornado.escape import utf8
from asyncio import Future
from http_client import RequestEngineBuilder, RequestEngine, RequestResult, FailFastError, ResponseData
from http_client.options import options
from http_client.util import response_from_debug

try:
    from tornado.stack_context import wrap
except ImportError:
    wrap = lambda f: f


http_client_logger = logging.getLogger('http_client')


class Server:
    STAT_LIMIT = 10_000_000

    @staticmethod
    def calculate_max_real_stat_load(servers):
        return max(server.calculate_load() for server in servers if server is not None)

    def __init__(self, address, weight=1, dc=None):
        self.address = address.rstrip('/')
        self.weight = int(weight)
        self.datacenter = dc

        self.current_requests = 0
        self.stat_requests = 0

        self.slow_start_mode_enabled = False
        self.slow_start_end_time = 0

        self.statistics_filled_with_initial_values = False

        if self.weight < 1:
            raise ValueError('weight should not be less then 1')

    def update(self, server):
        if self.weight != server.weight:
            ratio = float(server.weight) / float(self.weight)
            self.stat_requests = int(self.stat_requests * ratio)

        self.weight = server.weight
        self.datacenter = server.datacenter

    def set_slow_start_end_time_if_needed(self, slow_start_interval):
        if slow_start_interval > 0 and self.slow_start_end_time == 0:
            self.slow_start_mode_enabled = True
            self.slow_start_end_time = time.time() + random() * slow_start_interval

    def acquire(self):
        self.stat_requests += 1
        self.current_requests += 1

    def release(self, is_retry):
        if is_retry:
            self.stat_requests = self.stat_requests - 1 if self.stat_requests > 0 else 0
        self.current_requests = self.current_requests - 1 if self.current_requests > 0 else 0

    def get_stat_load(self, current_servers):
        if self.slow_start_mode_enabled:
            current_time = time.time()
            if self.slow_start_end_time > 0 and current_time <= self.slow_start_end_time:
                http_client_logger.debug(f'Server {self} is on slowStart, returning infinite load. '
                                         f'Current time: {current_time}, '
                                         f'slow start end time: {self.slow_start_end_time}')
                return float('inf')
            http_client_logger.debug(f'Slow start for server {self} ended')
            self.slow_start_mode_enabled = False

        if not self.statistics_filled_with_initial_values:
            self.statistics_filled_with_initial_values = True
            self.slow_start_end_time = -1
            initial_stat_requests = int(self.calculate_max_real_stat_load(current_servers) * self.weight)
            http_client_logger.debug(f'Server {self} statistics has no init value. '
                                     f'Calculated initial stat requests={initial_stat_requests}')
            self.stat_requests = initial_stat_requests

        return self.calculate_load()

    def calculate_load(self):
        return (self.stat_requests + self.current_requests) / float(self.weight)

    def need_to_rescale(self):
        return self.stat_requests >= self.STAT_LIMIT

    def rescale_stats_requests(self):
        self.stat_requests >>= 1

    def __str__(self) -> str:
        return f'{{address={self.address}, weight={self.weight}, datacenter={self.datacenter}, ' \
               f'current_requests={self.current_requests}, stat_requests={self.stat_requests}}}'


class Upstream:
    def __init__(self, name, config, servers):
        self.name = name
        self.servers = []
        self.max_tries = int(config.get('max_tries', options.http_client_default_max_tries))
        self.max_timeout_tries = int(config.get('max_timeout_tries', options.http_client_default_max_timeout_tries))
        self.connect_timeout = float(config.get('connect_timeout_sec', options.http_client_default_connect_timeout_sec))
        self.request_timeout = float(config.get('request_timeout_sec', options.http_client_default_request_timeout_sec))
        self.speculative_timeout_pct = float(config.get('speculative_timeout_pct', 0))
        self.slow_start_interval = float(config.get('slow_start_interval_sec', 0))
        self.retry_policy = RetryPolicy(config.get('retry_policy', {}))
        self.session_required = config.get('session_required', options.http_client_default_session_required)

        self._update_servers(servers)

    def acquire_server(self, excluded_servers=None):
        index = BalancingStrategy.get_least_loaded_server(self.servers, excluded_servers, options.datacenter,
                                                          options.http_client_allow_cross_datacenter_requests)

        if index is None:
            return None, None
        else:
            server = self.servers[index]
            server.acquire()
            return server.address, server.datacenter

    def release_server(self, host, is_retry=False):
        server = next((server for server in self.servers if server is not None and server.address == host), None)
        if server is not None:
            server.release(is_retry)
        self.rescale(self.servers)

    @staticmethod
    def rescale(servers):
        rescale = [True, options.http_client_allow_cross_datacenter_requests]

        for server in servers:
            if server is not None:
                local_or_remote = 0 if server.datacenter == options.datacenter else 1
                rescale[local_or_remote] &= server.need_to_rescale()

        if rescale[0] or rescale[1]:
            for server in servers:
                if server is not None:
                    local_or_remote = 0 if server.datacenter == options.datacenter else 1
                    if rescale[local_or_remote]:
                        server.rescale_stats_requests()

    def update(self, upstream):
        servers = upstream.servers

        self.max_tries = upstream.max_tries
        self.max_timeout_tries = upstream.max_timeout_tries
        self.connect_timeout = upstream.connect_timeout
        self.request_timeout = upstream.request_timeout
        self.speculative_timeout_pct = upstream.speculative_timeout_pct
        self.slow_start_interval = upstream.slow_start_interval
        self.retry_policy = upstream.retry_policy
        self.session_required = upstream.session_required

        self._update_servers(servers)

    def _update_servers(self, servers):
        mapping = {server.address: server for server in servers}

        for index, server in enumerate(self.servers):
            if server is None:
                continue

            changed = mapping.get(server.address)
            if changed is None:
                self.servers[index] = None
            else:
                del mapping[server.address]
                server.update(changed)

        for server in servers:
            if server.address in mapping:
                self._add_server(server)

    def _add_server(self, server):
        server.set_slow_start_end_time_if_needed(self.slow_start_interval)
        for index, s in enumerate(self.servers):
            if s is None:
                self.servers[index] = server
                return

        self.servers.append(server)

    def __str__(self):
        return '[{}]'.format(','.join(server.address for server in self.servers if server is not None))


class UpstreamManager:
    def __init__(self, upstreams=None):
        self.upstreams = {} if upstreams is None else upstreams

    def update_upstreams(self, upstreams):
        for upstream in upstreams:
            self.update_upstream(upstream)

    def update_upstream(self, upstream):
        current_upstream = self.upstreams.get(upstream.name)

        if current_upstream is None:
            shuffle(upstream.servers)
            self.upstreams[upstream.name] = upstream
            http_client_logger.debug('add %s upstream: %s', upstream.name, str(upstream))
            return

        current_upstream.update(upstream)
        http_client_logger.debug('update %s upstream: %s', upstream.name, str(upstream))

    def get_upstream(self, name):
        return self.upstreams.get(name)


class RetryPolicy:

    def __init__(self, properties=None):
        self.statuses = {}
        if properties:
            for status, config in properties.items():
                self.statuses[int(status)] = config.get('idempotent', 'false') == 'true'
        else:
            self.statuses = options.http_client_default_retry_policy

    def is_retriable(self, response, idempotent):
        if response.code == 599:
            error = str(response.error)
            if error.startswith('HTTP 599: Failed to connect') or error.startswith('HTTP 599: Connection timed out') \
                    or error.startswith('HTTP 599: Connection timeout'):
                return True

        if response.code not in self.statuses:
            return False

        return idempotent or self.statuses.get(response.code)


class BalancingStrategy:
    @staticmethod
    def get_least_loaded_server(servers, excluded_servers, current_datacenter, allow_cross_dc_requests):
        min_index = None
        min_weight = None

        for index, server in enumerate(servers):
            if server is None:
                continue

            is_different_dc = server.datacenter != current_datacenter

            if is_different_dc and not allow_cross_dc_requests:
                continue

            stat_load = server.get_stat_load(servers)
            weight = (excluded_servers is not None and server.address in excluded_servers, is_different_dc, stat_load)

            if min_index is None or min_weight > weight:
                min_index = index
                min_weight = weight

        return min_index


class ImmediateResultOrPreparedRequest:
    def __init__(self, processed_request: HTTPRequest = None, result: 'Future[RequestResult]' = None):
        self.result = result
        self.processed_request = processed_request


class BalancingState:

    def __init__(self, upstream: Upstream):
        self.upstream = upstream
        self.tried_servers = set()
        self.current_host = None
        self.current_datacenter = None

    def is_server_available(self):
        return self.current_host is not None

    def increment_tries(self):
        if self.is_server_available():
            self.tried_servers.add(self.current_host)
            self.current_host = None
            self.current_datacenter = None

    def acquire_server(self):
        host, datacenter = self.upstream.acquire_server(self.tried_servers)
        self.current_host = host
        self.current_datacenter = datacenter

    def release_server(self):
        if self.is_server_available():
            self.upstream.release_server(self.current_host, len(self.tried_servers) > 0)


class RequestBalancer(RequestEngine):

    @staticmethod
    def get_url(request):
        return f'http://{request.host}{request.uri}'

    def __init__(self, request: HTTPRequest, execute_request, modify_http_request_hook, debug_mode, callback,
                 parse_response, parse_on_error, fail_fast, connect_timeout, request_timeout, max_timeout_tries,
                 max_tries, speculative_timeout_pct, session_required, statsd_client, kafka_producer):

        trues = ('true', 'True', '1', True)
        request.session_required = session_required in trues

        request.connect_timeout = connect_timeout if request.connect_timeout is None else request.connect_timeout
        request.request_timeout = request_timeout if request.request_timeout is None else request.request_timeout

        request.connect_timeout *= options.timeout_multiplier
        request.request_timeout *= options.timeout_multiplier

        self.request = request
        self.execute_request = execute_request
        self.modify_http_request_hook = modify_http_request_hook

        max_timeout_tries = max_timeout_tries if request.max_timeout_tries is None else request.max_timeout_tries
        self.request.request_time_left = request.request_timeout * max_timeout_tries

        self.max_tries = max_tries
        self.tries_left = self.max_tries

        self.speculative_timeout_pct = speculative_timeout_pct if request.speculative_timeout_pct is None \
            else request.speculative_timeout_pct
        self.speculative_timeout = request.request_timeout * self.speculative_timeout_pct

        self.trace = OrderedDict()

        self.debug_mode = debug_mode
        self.callback = callback
        self.parse_response = parse_response
        self.parse_on_error = parse_on_error
        self.fail_fast = fail_fast

        self.statsd_client = statsd_client
        self.kafka_producer = kafka_producer

    def execute(self) -> 'Future[RequestResult]':
        future = Future()

        def request_finished_callback(response):
            if future.done():
                return

            result = RequestResult(self.request, response, self.parse_response, self.parse_on_error)

            if callable(self.callback):
                wrap(self.callback)(result.data, result.response)

            if self.fail_fast and result.failed:
                future.set_exception(FailFastError(result))
                return
            future.set_result(result)

        def retry_callback(response_future):
            exc = response_future.exception()
            if isinstance(exc, Exception):
                if isinstance(exc, HTTPError):
                    response = exc.response
                else:
                    future.set_exception(exc)
                    return
            else:
                response = response_future.result()

            self._update_left_tries_and_time(response)
            self.trace[response.request.host] = ResponseData(response.code, str(response.error))
            self._on_request_received()

            request = response.request
            tries_used = self.max_tries - self.tries_left
            retries_count = tries_used - 1
            response, debug_extra = self._unwrap_debug(request, response, retries_count)

            do_retry = self._check_retry(response, self.request.idempotent)

            self._log_response(response, retries_count, do_retry, debug_extra)
            self._send_response_metrics(response, tries_used, do_retry)

            if do_retry:
                self._on_retry()
                IOLoop.current().add_future(self._fetch(), retry_callback)
                return

            request_finished_callback(response)

        def speculative_retry():
            if future.done():
                return
            do_retry = self._check_speculative_retry()
            if do_retry:
                IOLoop.current().add_future(self._fetch(), retry_callback)
                return

        if callable(self.modify_http_request_hook):
            self.modify_http_request_hook(self.request)

        IOLoop.current().add_future(self._fetch(), retry_callback)
        if self._enable_speculative_retry():
            IOLoop.current().call_later(self.speculative_timeout, speculative_retry)

        return future

    def _get_result_or_context(self, request: HTTPRequest) -> ImmediateResultOrPreparedRequest:
        pass

    def _on_request_received(self):
        pass

    def _fetch(self):
        result_or_context = self._get_result_or_context(self.request)
        return self.execute_request(result_or_context.processed_request) if result_or_context.result is None \
            else result_or_context.result

    def _update_left_tries_and_time(self, response):
        self.request.request_time_left = self.request.request_time_left - response.request_time \
            if self.request.request_time_left >= response.request_time else 0
        if self.tries_left > 0:
            self.tries_left -= 1

    def _check_retry(self, response, is_idempotent):
        return self.tries_left > 0 and self.request.request_time_left > 0

    def _on_retry(self):
        pass

    def _enable_speculative_retry(self):
        return 0 < self.speculative_timeout_pct < 1

    def _check_speculative_retry(self):
        return self.request.idempotent and self.tries_left > 0

    def _unwrap_debug(self, request, response, retries_count):
        debug_extra = {}

        try:
            if response.headers.get('X-Hh-Debug'):
                debug_response = response_from_debug(request, response)
                if debug_response is not None:
                    debug_xml, response = debug_response
                    debug_extra['_debug_response'] = debug_xml

            if self.debug_mode:
                debug_extra.update({
                    '_response': response,
                    '_request': request,
                    '_request_retry': retries_count,
                    '_datacenter': response.request.upstream_datacenter,
                })
        except Exception:
            http_client_logger.exception('Cannot get response from debug')

        return response, debug_extra

    def _log_response(self, response, retries_count, do_retry, debug_extra):
        size = f' {len(response.body)} bytes' if response.body is not None else ''
        is_server_error = response.code >= 500
        request = response.request
        if do_retry:
            retry = f' on retry {retries_count}' if retries_count > 0 else ''
            log_message = f'balanced_request_response: {response.code} got {size}{retry}, will retry ' \
                          f'{request.method} {response.effective_url} in {response.request_time * 1000:.2f}ms'
            log_method = http_client_logger.info if is_server_error else http_client_logger.debug
        else:
            msg_label = 'balanced_request_final_error' if is_server_error else 'balanced_request_final_response'
            log_message = f'{msg_label}: {response.code} got {size} ' \
                          f'{request.method} ' \
                          f'{self.get_url(request)}, ' \
                          f'trace: {self.get_trace()}'

            log_method = http_client_logger.warning if is_server_error else http_client_logger.info

        log_method(log_message, extra=debug_extra)

        if response.code == 599:
            timings_info = (f'{stage}={timing * 1000:.3f}ms' for stage, timing in response.time_info.items())
            http_client_logger.info('Curl timings: %s', ' '.join(timings_info))

    def _send_response_metrics(self, response, tries_used, do_retry):
        request = response.request

        if self.statsd_client is not None:
            self.statsd_client.stack()
            self.statsd_client.count(
                'http.client.requests', 1,
                upstream=request.upstream_name,
                dc=request.upstream_datacenter,
                final='false' if do_retry else 'true',
                status=response.code
            )
            self.statsd_client.time(
                'http.client.request.time',
                int(response.request_time * 1000),
                dc=request.upstream_datacenter,
                upstream=request.upstream_name
            )
            if not do_retry and tries_used > 1:
                self.statsd_client.count(
                    'http.client.retries', 1,
                    upstream=request.upstream_name,
                    dc=request.upstream_datacenter,
                    first_status=next(iter(self.trace.values())).responseCode,
                    tries=tries_used,
                    status=response.code
                )
            self.statsd_client.flush()

        if self.kafka_producer is not None and not do_retry:
            dc = request.upstream_datacenter or options.datacenter or 'unknown'
            current_host = request.host or 'unknown'
            request_id = request.headers.get('X-Request-Id', 'unknown')
            upstream = request.upstream_name or 'unknown'

            asyncio.get_event_loop().create_task(self.kafka_producer.send(
                'metrics_requests',
                utf8(f'{{"app":"{options.app}","dc":"{dc}","hostname":"{current_host}","requestId":"{request_id}",'
                     f'"status":{response.code},"ts":{int(time.time())},"upstream":"{upstream}"}}')
            ))

    def get_trace(self):
        return ' -> '.join([f'{host}~{data.responseCode}~{data.msg}'
                            for host, data in self.trace.items()])


class ExternalUrlRequestor(RequestBalancer):
    DC_FOR_EXTERNAL_REQUESTS = "externalRequest"
    DEFAULT_RETRY_POLICY = RetryPolicy()

    def __init__(self, request: HTTPRequest, execute_request, modify_http_request_hook, debug_mode, callback,
                 parse_response, parse_on_error, fail_fast, statsd_client=None, kafka_producer=None):
        super().__init__(request, execute_request, modify_http_request_hook, debug_mode, callback, parse_response,
                         parse_on_error, fail_fast, options.http_client_default_connect_timeout_sec,
                         options.http_client_default_request_timeout_sec, options.http_client_default_max_timeout_tries,
                         options.http_client_default_max_tries, 0, options.http_client_default_session_required,
                         statsd_client, kafka_producer)

    def _get_result_or_context(self, request: HTTPRequest):
        request.upstream_datacenter = self.DC_FOR_EXTERNAL_REQUESTS
        return ImmediateResultOrPreparedRequest(processed_request=request)

    def _check_retry(self, response, is_idempotent):
        do_retry = super()._check_retry(response, is_idempotent)
        return do_retry and self.DEFAULT_RETRY_POLICY.is_retriable(response, is_idempotent)


class UpstreamRequestBalancer(RequestBalancer):

    @staticmethod
    def _get_server_not_available_result(request: HTTPRequest, upstream_name):
        future = Future()
        future.set_result(HTTPResponse(
            request, 502, error=HTTPError(502, 'No available servers for upstream: ' + upstream_name),
            request_time=0
        ))
        return future

    def __init__(self, state: BalancingState, request: HTTPRequest, execute_request, modify_http_request_hook,
                 debug_mode, callback, parse_response, parse_on_error, fail_fast,
                 statsd_client=None, kafka_producer=None):
        super().__init__(request, execute_request, modify_http_request_hook, debug_mode, callback, parse_response,
                         parse_on_error, fail_fast, state.upstream.connect_timeout, state.upstream.request_timeout,
                         state.upstream.max_timeout_tries, state.upstream.max_tries,
                         state.upstream.speculative_timeout_pct, state.upstream.session_required,
                         statsd_client, kafka_producer)
        self.state = state

    def _get_result_or_context(self, request: HTTPRequest):
        upstream_name = self.state.upstream.name
        self.state.acquire_server()
        if not self.state.is_server_available():
            result = self._get_server_not_available_result(request, upstream_name)
            return ImmediateResultOrPreparedRequest(result=result)

        request.host = self.state.current_host
        request.url = self.state.current_host + self.request.uri
        request.upstream_datacenter = self.state.current_datacenter

        return ImmediateResultOrPreparedRequest(processed_request=request)

    def _on_request_received(self):
        self.state.release_server()

    def _check_retry(self, response, is_idempotent):
        do_retry = super()._check_retry(response, is_idempotent)
        return do_retry and self.state.upstream.retry_policy.is_retriable(response, is_idempotent)

    def _on_retry(self):
        self.state.increment_tries()


class RequestBalancerBuilder(RequestEngineBuilder):

    def __init__(self, upstream_manager: UpstreamManager, statsd_client=None, kafka_producer=None):
        self.upstream_manager = upstream_manager
        self.statsd_client = statsd_client
        self.kafka_producer = kafka_producer

    def build(self, request: HTTPRequest, execute_request, modify_http_request_hook, debug_mode, callback,
              parse_response, parse_on_error, fail_fast) -> RequestEngine:
        upstream = self.upstream_manager.get_upstream(request.host)
        if upstream is None:
            return ExternalUrlRequestor(request, execute_request, modify_http_request_hook, debug_mode, callback,
                                        parse_response, parse_on_error, fail_fast,
                                        self.statsd_client, self.kafka_producer)
        else:
            state = BalancingState(upstream)
            return UpstreamRequestBalancer(state, request, execute_request, modify_http_request_hook, debug_mode,
                                           callback, parse_response, parse_on_error, fail_fast,
                                           self.statsd_client, self.kafka_producer)
