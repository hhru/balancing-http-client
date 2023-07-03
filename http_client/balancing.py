import asyncio
import logging
import time
from asyncio import Future
from collections import OrderedDict
from random import random, shuffle

import aiohttp
from aiohttp.client_exceptions import ClientConnectorError, ServerTimeoutError

from http_client import (RequestBuilder, RequestEngine, RequestEngineBuilder,
                         RequestResult)
from http_client.options import options
from http_client.request_response import (FailFastError,
                                          NoAvailableServerException,
                                          ResponseData)
from http_client.util import utf8

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


class RetryPolicy:

    def __init__(self, properties=None):
        self.statuses = {}
        if properties:
            for status, config in properties.items():
                self.statuses[int(status)] = config.get('idempotent', 'false') == 'true'
        else:
            self.statuses = options.http_client_default_retry_policy

    def is_retriable(self, result: RequestResult, idempotent):
        if isinstance(result.exc, (ClientConnectorError, ServerTimeoutError)):
            return True

        if result.exc is not None and idempotent:
            return True

        if result.status_code not in self.statuses:
            return False

        return idempotent or self.statuses.get(result.status_code)


class UpstreamConfig:

    def __init__(self, max_tries=None,
                 max_timeout_tries=None,
                 connect_timeout=None,
                 request_timeout=None,
                 speculative_timeout_pct=None,
                 slow_start_interval=None,
                 retry_policy=None,
                 session_required=None):
        self.max_tries = int(options.http_client_default_max_tries if max_tries is None else max_tries)
        self.max_timeout_tries = int(options.http_client_default_max_timeout_tries if max_timeout_tries is None
                                     else max_timeout_tries)
        self.connect_timeout = float(options.http_client_default_connect_timeout_sec if connect_timeout is None
                                     else connect_timeout)
        self.request_timeout = float(options.http_client_default_request_timeout_sec if request_timeout is None
                                     else request_timeout)
        self.speculative_timeout_pct = float(0 if speculative_timeout_pct is None else speculative_timeout_pct)
        self.slow_start_interval = float(0 if slow_start_interval is None else slow_start_interval)
        self.retry_policy = RetryPolicy({} if retry_policy is None else retry_policy)
        trues = ('true', 'True', '1', True)
        self.session_required = (options.http_client_default_session_required if session_required is None
                                 else session_required) in trues

    def __repr__(self):
        return f'{{"max_tries":{self.max_tries}, "max_timeout_tries":{self.max_timeout_tries}, ' \
               f'"connect_timeout":{self.connect_timeout}, "request_timeout":{self.request_timeout}, ' \
               f'"speculative_timeout_pct":{self.speculative_timeout_pct}, ' \
               f'"slow_start_interval":{self.slow_start_interval}, "session_required":{self.session_required}}}'


class Upstream:

    DEFAULT_PROFILE = "default"

    def __init__(self, name, config_by_profile, servers):
        self.name = name
        self.servers = []
        self.config_by_profile = config_by_profile if config_by_profile \
            else {Upstream.DEFAULT_PROFILE: self.get_default_config()}
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
        self.config_by_profile = upstream.config_by_profile
        self._update_servers(upstream.servers)

    def get_config(self, profile):
        if not profile:
            profile = Upstream.DEFAULT_PROFILE
        config = self.config_by_profile.get(profile)
        if config is None:
            raise ValueError(f'Profile {profile} should be present')
        return config

    @staticmethod
    def get_default_config():
        return UpstreamConfig()

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
        slow_start_interval = self.config_by_profile.get(Upstream.DEFAULT_PROFILE).slow_start_interval
        server.set_slow_start_end_time_if_needed(slow_start_interval)
        for index, s in enumerate(self.servers):
            if s is None:
                self.servers[index] = server
                return

        self.servers.append(server)

    def __str__(self):
        return '[{}]'.format(','.join(server.address for server in self.servers if server is not None))


class UpstreamManager:
    def __init__(self, upstreams=None):
        if isinstance(upstreams, dict):
            self.upstreams = upstreams
        elif isinstance(upstreams, list):
            self.upstreams = {upstream.name: upstream for upstream in upstreams}
        else:
            self.upstreams = {}

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
    def __init__(self, processed_request: RequestBuilder = None, result: RequestResult = None):
        self.result = result
        self.processed_request = processed_request


class BalancingState:

    def __init__(self, upstream: Upstream, profile):
        self.upstream = upstream
        self.profile = profile
        self.tried_servers = set()
        self.current_host = None
        self.current_datacenter = None

    def get_upstream_config(self):
        return self.upstream.get_config(self.profile)

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
    def __init__(self, request: RequestBuilder, execute_request, modify_http_request_hook, debug_mode,
                 parse_response, parse_on_error, fail_fast, connect_timeout, request_timeout, max_timeout_tries,
                 max_tries, speculative_timeout_pct, session_required, statsd_client, kafka_producer):

        request.session_required = session_required

        request.connect_timeout = connect_timeout if request.connect_timeout is None else request.connect_timeout
        request.request_timeout = request_timeout if request.request_timeout is None else request.request_timeout
        request.connect_timeout *= options.timeout_multiplier
        request.request_timeout *= options.timeout_multiplier
        request.timeout = aiohttp.ClientTimeout(total=request.request_timeout, connect=request.connect_timeout)

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
        self.parse_response = parse_response
        self.parse_on_error = parse_on_error
        self.fail_fast = fail_fast

        self.statsd_client = statsd_client
        self.kafka_producer = kafka_producer

    async def speculative_retry(self):
        await asyncio.sleep(self.speculative_timeout)

        if self._check_speculative_retry():
            return await self._retryable_fetch()

    def execute(self) -> Future[RequestResult]:
        if callable(self.modify_http_request_hook):
            self.modify_http_request_hook(self.request)

        request_task = asyncio.create_task(self._retryable_fetch())
        if not self._enable_speculative_retry():
            return request_task

        speculative_request_task = asyncio.create_task(self.speculative_retry())
        result_task = asyncio.create_task(speculative_requests(request_task, speculative_request_task))
        return result_task

    async def _retryable_fetch(self) -> RequestResult:
        result_or_request = self._get_result_or_context(self.request)  # modify self.request
        if result_or_request.result is not None:
            result = result_or_request.result
        else:
            result: RequestResult = await self.execute_request(result_or_request.processed_request)

        result.parse_response = self.parse_response
        result.parse_on_error = self.parse_on_error

        self._update_left_tries_and_time(result.elapsed_time)
        self.trace[self.request.host] = ResponseData(result.status_code, result.error)
        self._on_response_received()

        tries_used = self.max_tries - self.tries_left
        retries_count = tries_used - 1
        result, debug_extra = self._unwrap_debug(self.request, result, retries_count)

        do_retry = self._check_retry(result, self.request.idempotent)

        self._log_response(result, retries_count, do_retry, debug_extra)
        self._send_response_metrics(result, tries_used, do_retry)

        if do_retry:
            self._on_retry()
            result = await self._retryable_fetch()

        if self.fail_fast and result.failed:
            raise FailFastError(result)

        return result

    def _get_result_or_context(self, request: RequestBuilder) -> ImmediateResultOrPreparedRequest:
        raise NotImplementedError

    def _on_response_received(self):
        pass

    def _update_left_tries_and_time(self, elapsed_time: float):
        self.request.request_time_left = self.request.request_time_left - elapsed_time \
            if self.request.request_time_left >= elapsed_time else 0
        if self.tries_left > 0:
            self.tries_left -= 1

    def _check_retry(self, response: RequestResult, is_idempotent):
        return self.tries_left > 0 and self.request.request_time_left > 0

    def _on_retry(self):
        pass

    def _enable_speculative_retry(self):
        return 0 < self.speculative_timeout_pct < 1

    def _check_speculative_retry(self):
        return self.request.idempotent and self.tries_left > 0

    def _unwrap_debug(self, request, result: RequestResult, retries_count):
        debug_extra = {}

        try:
            if result.headers.get('X-Hh-Debug'):
                debug_response = result.response_from_debug()
                if debug_response is not None:
                    debug_xml, result = debug_response
                    debug_extra['_debug_response'] = debug_xml

            if self.debug_mode:
                debug_extra.update({
                    '_response': result,
                    '_request': request,
                    '_request_retry': retries_count,
                    '_datacenter': result.request.upstream_datacenter,
                })
        except Exception:
            http_client_logger.exception('Cannot get response from debug')

        return result, debug_extra

    def _log_response(self, result: RequestResult, retries_count, do_retry, debug_extra):
        body_bytes = result.get_body_length()
        size = f' {body_bytes} bytes' if body_bytes is not None else ''
        is_server_error = result.exc is not None or result.status_code >= 500
        request = self.request
        if do_retry:
            effective_url = request.url if result.exc is not None else result._response.real_url
            retry = f' on retry {retries_count}' if retries_count > 0 else ''
            log_message = f'balanced_request_response: {result.status_code} got {size}{retry}, will retry ' \
                          f'{request.method} {effective_url} in {result.elapsed_time * 1000:.2f}ms'
            log_method = http_client_logger.info if is_server_error else http_client_logger.debug
        else:
            msg_label = 'balanced_request_final_error' if is_server_error else 'balanced_request_final_response'
            log_message = f'{msg_label}: {result.status_code} got {size} ' \
                          f'{request.method} ' \
                          f'{request.url}, ' \
                          f'trace: {self.get_trace()}'

            log_method = http_client_logger.warning if is_server_error else http_client_logger.info
        log_method(log_message, extra=debug_extra)

    def _send_response_metrics(self, result, tries_used, do_retry):
        request = self.request

        if self.statsd_client is not None:
            self.statsd_client.stack()
            self.statsd_client.count(
                'http.client.requests', 1,
                upstream=request.upstream_name,
                dc=request.upstream_datacenter,
                final='false' if do_retry else 'true',
                status=result.status_code
            )
            self.statsd_client.time(
                'http.client.request.time',
                int(result.elapsed_time * 1000),
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
                    status=result.status_code
                )
            self.statsd_client.flush()

        if self.kafka_producer is not None and not do_retry:
            dc = request.upstream_datacenter or options.datacenter or 'unknown'
            current_host = request.host or 'unknown'
            request_id = result.headers.get('X-Request-Id', 'unknown')
            status_code = result.status_code or 'null'
            upstream = request.upstream_name or 'unknown'

            asyncio.get_event_loop().create_task(self.kafka_producer.send(
                'metrics_requests',
                utf8(f'{{"app":"{options.app}","dc":"{dc}","hostname":"{current_host}","requestId":"{request_id}",'
                     f'"status":{status_code},"ts":{int(time.time())},"upstream":"{upstream}"}}')
            ))

    def get_trace(self):
        return ' -> '.join([f'{host}~{data.responseCode}~{data.msg}'
                            for host, data in self.trace.items()])


class ExternalUrlRequestor(RequestBalancer):
    DC_FOR_EXTERNAL_REQUESTS = "externalRequest"
    DEFAULT_RETRY_POLICY = RetryPolicy()

    def __init__(self, request: RequestBuilder, execute_request, modify_http_request_hook, debug_mode,
                 parse_response, parse_on_error, fail_fast, statsd_client=None, kafka_producer=None):
        default_config = Upstream.get_default_config()
        super().__init__(request, execute_request, modify_http_request_hook, debug_mode, parse_response,
                         parse_on_error, fail_fast, default_config.connect_timeout, default_config.request_timeout,
                         default_config.max_timeout_tries, default_config.max_tries,
                         default_config.speculative_timeout_pct, default_config.session_required,
                         statsd_client, kafka_producer)

    def _get_result_or_context(self, request: RequestBuilder):
        request.upstream_datacenter = self.DC_FOR_EXTERNAL_REQUESTS
        return ImmediateResultOrPreparedRequest(processed_request=request)

    def _check_retry(self, response: RequestResult, is_idempotent):
        do_retry = super()._check_retry(response, is_idempotent)
        return do_retry and self.DEFAULT_RETRY_POLICY.is_retriable(response, is_idempotent)


class UpstreamRequestBalancer(RequestBalancer):

    @staticmethod
    def _get_server_not_available_result(request: RequestBuilder, upstream_name) -> RequestResult:
        exc = NoAvailableServerException(f'No available servers for upstream: {upstream_name}')
        return RequestResult(request, exc=exc, elapsed_time=0)

    def __init__(self, state: BalancingState, request: RequestBuilder, execute_request, modify_http_request_hook,
                 debug_mode, parse_response, parse_on_error, fail_fast,
                 statsd_client=None, kafka_producer=None):
        upstream_config = state.get_upstream_config()
        super().__init__(request, execute_request, modify_http_request_hook, debug_mode, parse_response,
                         parse_on_error, fail_fast, upstream_config.connect_timeout, upstream_config.request_timeout,
                         upstream_config.max_timeout_tries, upstream_config.max_tries,
                         upstream_config.speculative_timeout_pct, upstream_config.session_required,
                         statsd_client, kafka_producer)
        self.state = state

    def _get_result_or_context(self, request: RequestBuilder):
        upstream_name = self.state.upstream.name
        self.state.acquire_server()
        if not self.state.is_server_available():
            result = self._get_server_not_available_result(request, upstream_name)
            return ImmediateResultOrPreparedRequest(result=result)

        request.host = self.state.current_host
        request.url = f'http://{self.state.current_host}{self.request.path}'
        request.upstream_datacenter = self.state.current_datacenter

        return ImmediateResultOrPreparedRequest(processed_request=request)

    def _on_response_received(self):
        self.state.release_server()

    def _check_retry(self, response: RequestResult, is_idempotent):
        do_retry = super()._check_retry(response, is_idempotent)
        return do_retry and self.state.get_upstream_config().retry_policy.is_retriable(response, is_idempotent)

    def _on_retry(self):
        self.state.increment_tries()


class RequestBalancerBuilder(RequestEngineBuilder):

    def __init__(self, upstream_manager: UpstreamManager, statsd_client=None, kafka_producer=None):
        self.upstream_manager = upstream_manager
        self.statsd_client = statsd_client
        self.kafka_producer = kafka_producer

    def build(self, request: RequestBuilder, profile, execute_request, modify_http_request_hook, debug_mode,
              parse_response, parse_on_error, fail_fast) -> RequestEngine:
        upstream = self.upstream_manager.get_upstream(request.host)
        if upstream is None:
            return ExternalUrlRequestor(request, execute_request, modify_http_request_hook, debug_mode,
                                        parse_response, parse_on_error, fail_fast,
                                        self.statsd_client, self.kafka_producer)
        else:
            state = BalancingState(upstream, profile)
            return UpstreamRequestBalancer(state, request, execute_request, modify_http_request_hook, debug_mode,
                                           parse_response, parse_on_error, fail_fast,
                                           self.statsd_client, self.kafka_producer)


async def speculative_requests(request: asyncio.Task, speculative_request: asyncio.Task) -> RequestResult:
    done, pending = await asyncio.wait([request, speculative_request], return_when=asyncio.FIRST_COMPLETED)

    if request in done and not request.result().failed:
        speculative_request.cancel()
        return request.result()

    if speculative_request in done and speculative_request.result() is not None and \
            not speculative_request.result().failed:
        request.cancel()
        return speculative_request.result()

    for task in pending:
        await task

    if not request.result().failed or speculative_request.result() is None or speculative_request.result().failed:
        return request.result()

    return speculative_request.result()
