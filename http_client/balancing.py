import asyncio
import collections
import logging
import time
from asyncio import Future
from collections import OrderedDict
from random import random
from typing import List

import aiohttp
from aiohttp.client_exceptions import ClientConnectorError, ServerTimeoutError

from http_client import (RequestBuilder, RequestEngine, RequestEngineBuilder,
                         RequestResult)
from http_client.options import options
from http_client.request_response import (FailFastError,
                                          NoAvailableServerException,
                                          ResponseData)
from http_client.util import utf8, weighted_sample


DOWNTIME_DETECTOR_WINDOW = 100
RESPONSE_TIME_TRACKER_WINDOW = 500
WARM_UP_DEFAULT_TIME_MILLIS = 100
LOWEST_HEALTH_PERCENT = 2
LOWEST_HEALTH = int(LOWEST_HEALTH_PERCENT * DOWNTIME_DETECTOR_WINDOW / 100)
INITIAL_LIVE_PERCENT = 10

http_client_logger = logging.getLogger('http_client')


class DowntimeDetector:
    def __init__(self, max_length=DOWNTIME_DETECTOR_WINDOW, initial_live_percent=INITIAL_LIVE_PERCENT):
        self.max_length = max_length
        if initial_live_percent > 0:
            self.healths = collections.deque(maxlen=max_length)
            ones = max_length * initial_live_percent // 100
            self.healths.extend([0] * (max_length - ones))
            self.healths.extend([1] * ones)
            self.health = ones
        else:
            self.healths = collections.deque([1], maxlen=max_length)
            self.health = max_length

    def add_fail(self):
        self.health -= self.healths[0]
        self.healths.append(0)

    def add_success(self):
        self.health += 1 - self.healths[0]
        self.healths.append(1)


class ResponseTimeTracker:
    def __init__(self, max_length=RESPONSE_TIME_TRACKER_WINDOW):
        self.is_warm_up = True
        self.total = 0
        self.max_length = max_length
        self.response_times = collections.deque([0], max_length)
        self.mean = 1

    def add_response_time(self, time_ms: int):
        self.total += time_ms - self.response_times[0]
        self.mean = (self.total // self.max_length) or 1

        self.response_times.append(time_ms)
        if len(self.response_times) == self.max_length:
            self.is_warm_up = False


class Server:
    STAT_LIMIT = 10_000_000

    @staticmethod
    def calculate_max_real_stat_load(servers):
        return max(server.calculate_load() for server in servers if server is not None)

    def __init__(self, address, hostname, weight=1, dc=None):
        self.address = address.rstrip('/')
        self.hostname: str = hostname
        self.weight = int(weight)
        self.datacenter: str = dc

        self.current_requests = 0
        self.stat_requests = 0

        self.slow_start_mode_enabled = False
        self.slow_start_end_time = 0

        self.statistics_filled_with_initial_values = False

        self.downtime_detector = DowntimeDetector()
        self.response_time_tracker = ResponseTimeTracker()

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

    def release_adaptive(self, elapsed_time_s, is_server_error):
        if is_server_error:
            self.downtime_detector.add_fail()
        else:
            self.downtime_detector.add_success()
            elapsed_time_ms = int(elapsed_time_s * 1000)
            self.response_time_tracker.add_response_time(elapsed_time_ms)

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
                self.statuses[int(status)] = config.get('retry_non_idempotent', 'false') == 'true'
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

    def is_server_error(self, result: RequestResult):
        if isinstance(result.exc, (ClientConnectorError, ServerTimeoutError)):
            return True

        return result.status_code in self.statuses

    def __repr__(self):
        policy = [f'"{status}":{{"retry_non_idempotent":{self.statuses.get(status)}}}'
                  for status in sorted(self.statuses.keys())]

        return f'{{{", ".join(policy)}}}'


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
               f'"slow_start_interval":{self.slow_start_interval}, "session_required":{self.session_required}, ' \
               f'"retry_policy":{self.retry_policy}}}'


class Upstream:

    DEFAULT_PROFILE = "default"

    def __init__(self, name, config_by_profile, servers):
        self.name = name
        self.servers: List[Server] = []
        self.config_by_profile = config_by_profile if config_by_profile \
            else {Upstream.DEFAULT_PROFILE: self.get_default_config()}
        self._update_servers(servers)
        self.allow_cross_dc_requests = options.http_client_allow_cross_datacenter_requests \
            or name in options.force_allow_cross_datacenter_for_upstreams
        self.datacenter: str = options.datacenter

    def acquire_server(self, excluded_servers=None):
        index = BalancingStrategy.get_least_loaded_server(self.servers, excluded_servers, self.datacenter,
                                                          self.allow_cross_dc_requests)

        if index is None:
            return None, None, None
        else:
            server = self.servers[index]
            server.acquire()
            return server.address, server.datacenter, server.hostname

    def acquire_adaptive_servers(self, profile: str):
        allowed_servers = []
        for server in self.servers:
            if server is not None and (self.allow_cross_dc_requests or self.datacenter == server.datacenter):
                allowed_servers.append(server)

        chosen_servers = AdaptiveBalancingStrategy.get_servers(allowed_servers, self.get_config(profile).max_tries)
        return [(server.address, server.datacenter, server.hostname) for server in chosen_servers]

    def release_server(self, host, is_retry, elapsed_time, is_server_error, adaptive=False):
        server = next((server for server in self.servers if server is not None and server.address == host), None)
        if server is not None:
            if adaptive:
                server.release_adaptive(elapsed_time, is_server_error)
            else:
                server.release(is_retry)

        if not adaptive:
            self.rescale(self.servers)

    def rescale(self, servers):
        rescale = [True, self.allow_cross_dc_requests]

        for server in servers:
            if server is not None:
                local_or_remote = 0 if server.datacenter == self.datacenter else 1
                rescale[local_or_remote] &= server.need_to_rescale()

        if rescale[0] or rescale[1]:
            for server in servers:
                if server is not None:
                    local_or_remote = 0 if server.datacenter == self.datacenter else 1
                    if rescale[local_or_remote]:
                        server.rescale_stats_requests()

    def update(self, upstream):
        self.config_by_profile = upstream.config_by_profile
        self._update_servers(upstream.servers)

    def get_config(self, profile) -> UpstreamConfig:
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


class AdaptiveBalancingStrategy:
    @staticmethod
    def get_servers(servers: List[Server], max_tries: int) -> List[Server]:
        n = len(servers)
        count = min(n, max_tries)

        if n <= 1:
            return servers

        # gather statistics
        warmups = None
        sum_of_means = 0
        warmup_count = 0
        min_mean = 100500
        max_mean = 0
        for i, server in enumerate(servers):
            tracker: ResponseTimeTracker = server.response_time_tracker
            if options.log_adaptive_statistics:
                http_client_logger.debug('gathering stats %s, warm_up: %s, time: %s, successCount: %s', server,
                                         tracker.is_warm_up, tracker.mean, server.downtime_detector.health)

            if tracker.is_warm_up:
                if warmups is None:
                    warmups = [False] * n
                warmups[i] = True
                warmup_count += 1
            else:
                mean = tracker.mean
                if mean < min_mean:
                    min_mean = mean
                if mean > max_mean:
                    max_mean = mean
                sum_of_means += mean

        warmup_score = None
        if warmups is not None:
            if sum_of_means == 0:
                warmup_score = WARM_UP_DEFAULT_TIME_MILLIS
            else:
                warmup_score = sum_of_means / (n - warmup_count)

        # adjust scores based on downtime detector health and response time tracker score
        scores = []
        total = 0
        for i, server in enumerate(servers):
            if warmups is not None and warmups[i]:
                inverted_time = round(warmup_score)
            else:
                inverted_time = round(min_mean * max_mean / server.response_time_tracker.mean)

            score = inverted_time * max(server.downtime_detector.health, LOWEST_HEALTH)
            if options.log_adaptive_statistics:
                http_client_logger.debug('balancer stats for %s, health: %s, inverted_time_score: %s, final_score: %s',
                                         server, server.downtime_detector.health, inverted_time, score)
            scores.append(score)
            total += score

        return weighted_sample(servers, scores, count, total)


class ImmediateResultOrPreparedRequest:
    def __init__(self, processed_request: RequestBuilder = None, result: RequestResult = None):
        self.result = result
        self.processed_request = processed_request


class BalancingState:

    def __init__(self, upstream: Upstream, profile: str):
        self.upstream = upstream
        self.profile = profile
        self.tried_servers = set()
        self.current_host = None
        self.current_hostname = None
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
        host, datacenter, hostname = self.upstream.acquire_server(self.tried_servers)
        self.set_current_server(host, datacenter, hostname)

    def set_current_server(self, host, datacenter, hostname):
        self.current_host = host
        self.current_datacenter = datacenter
        self.current_hostname = hostname

    def release_server(self, elapsed_time, is_server_error):
        if self.is_server_available():
            self.upstream.release_server(self.current_host, len(self.tried_servers) > 0, elapsed_time, is_server_error)


class AdaptiveBalancingState(BalancingState):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.adaptive_failed = False
        self.server_entry_iterator = None

    def acquire_server(self):
        if not self.adaptive_failed:
            try:
                host, datacenter, hostname = self.acquire_adaptive_server()
                self.set_current_server(host, datacenter, hostname)
                return
            except Exception as exc:
                http_client_logger.error('failed to acquire adaptive servers, falling back to nonadaptive %s', exc)
                self.adaptive_failed = True
        super().acquire_server()

    def release_server(self, elapsed_time, is_server_error):
        if self.is_server_available():
            self.upstream.release_server(self.current_host, len(self.tried_servers) > 0, elapsed_time, is_server_error,
                                         not self.adaptive_failed)

    def acquire_adaptive_server(self):
        if self.server_entry_iterator is None:
            entries = self.upstream.acquire_adaptive_servers(self.profile)
            self.server_entry_iterator = iter(entries)

        return next(self.server_entry_iterator)


class RequestBalancer(RequestEngine):
    def __init__(self, request: RequestBuilder, execute_request, modify_http_request_hook, debug_enabled,
                 parse_response, parse_on_error, fail_fast, connect_timeout, request_timeout, max_timeout_tries,
                 max_tries, speculative_timeout_pct, session_required, statsd_client, kafka_producer):

        request.session_required = session_required

        request.connect_timeout = connect_timeout if request.connect_timeout is None else request.connect_timeout
        request.request_timeout = request_timeout if request.request_timeout is None else request.request_timeout
        request.connect_timeout *= options.timeout_multiplier
        request.request_timeout *= options.timeout_multiplier
        request.timeout = aiohttp.ClientTimeout(total=request.request_timeout, connect=request.connect_timeout)

        self.request: RequestBuilder = request
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

        self.debug_enabled = debug_enabled
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
        self._on_response_received(result)

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

    def _on_response_received(self, result: RequestResult):
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
        return self.request.idempotent and 0 < self.speculative_timeout_pct < 1

    def _check_speculative_retry(self):
        return self.tries_left > 0

    def _unwrap_debug(self, request, result: RequestResult, retries_count):
        debug_extra = {}

        try:
            if result.headers.get('X-Hh-Debug'):
                debug_response = result.response_from_debug()
                if debug_response is not None:
                    debug_xml, result = debug_response
                    debug_extra['_debug_response'] = debug_xml

            if self.debug_enabled:
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
        log_level = logging.WARNING
        if do_retry:
            url = self.request.url if result.exc is not None else str(result._response.real_url)
            retry = f' on retry {retries_count}' if retries_count > 0 else ''
            log_message = f'response: {result.status_code} got {size}{retry}, will retry ' \
                          f'{self.request.method} {url} in {result.elapsed_time * 1000:.2f}ms'
            if not is_server_error:
                log_level = logging.DEBUG
        else:
            url = self.request.url
            msg_label = 'final_error' if is_server_error else 'final_response'
            log_message = f'{msg_label}: {result.status_code} got {size} {self.request.method} {url}'

            if not is_server_error and retries_count == 0:
                log_level = logging.getLevelName(options.balancing_requests_log_level.upper())

        http_client_logger.log(log_level, log_message, extra=debug_extra)

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
                utf8(f'{{"app":"{request.source_app}","dc":"{dc}","hostname":"{current_host}",'
                     f'"requestId":"{request_id}","status":{status_code},"ts":{int(time.time())},'
                     f'"upstream":"{upstream}"}}')
            ))

    def get_trace(self):
        return ' -> '.join([f'{host}~{data.responseCode}~{data.msg}'
                            for host, data in self.trace.items()])


class ExternalUrlRequestor(RequestBalancer):
    DC_FOR_EXTERNAL_REQUESTS = "externalRequest"
    DEFAULT_RETRY_POLICY = RetryPolicy()

    def __init__(self, request: RequestBuilder, execute_request, modify_http_request_hook, debug_enabled,
                 parse_response, parse_on_error, fail_fast, statsd_client=None, kafka_producer=None):
        default_config = Upstream.get_default_config()
        super().__init__(request, execute_request, modify_http_request_hook, debug_enabled, parse_response,
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
        return RequestResult(request, 599, exc=exc, elapsed_time=0)

    def __init__(self, state: BalancingState, request: RequestBuilder, execute_request, modify_http_request_hook,
                 debug_enabled, parse_response, parse_on_error, fail_fast,
                 statsd_client=None, kafka_producer=None):
        upstream_config = state.get_upstream_config()
        super().__init__(request, execute_request, modify_http_request_hook, debug_enabled, parse_response,
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
        request.upstream_hostname = self.state.current_hostname

        return ImmediateResultOrPreparedRequest(processed_request=request)

    def _on_response_received(self, result):
        upstream_config = self.state.upstream.get_config(self.state.profile)
        is_server_error = upstream_config.retry_policy.is_server_error(result)
        self.state.release_server(result.elapsed_time, is_server_error)

    def _check_retry(self, response: RequestResult, is_idempotent):
        do_retry = super()._check_retry(response, is_idempotent)
        return do_retry and self.state.get_upstream_config().retry_policy.is_retriable(response, is_idempotent)

    def _on_retry(self):
        self.state.increment_tries()


class RequestBalancerBuilder(RequestEngineBuilder):

    def __init__(self, upstreams: dict[str, Upstream], statsd_client=None, kafka_producer=None):
        self._upstreams = upstreams
        self.statsd_client = statsd_client
        self.kafka_producer = kafka_producer

    def build(self, request: RequestBuilder, profile, execute_request, modify_http_request_hook, debug_enabled,
              parse_response, parse_on_error, fail_fast) -> RequestEngine:
        upstream = self._upstreams.get(request.host)
        if upstream is None:
            return ExternalUrlRequestor(request, execute_request, modify_http_request_hook, debug_enabled,
                                        parse_response, parse_on_error, fail_fast,
                                        self.statsd_client, self.kafka_producer)
        else:
            if options.http_client_adaptive_strategy:
                state = AdaptiveBalancingState(upstream, profile)
            else:
                state = BalancingState(upstream, profile)
            return UpstreamRequestBalancer(state, request, execute_request, modify_http_request_hook, debug_enabled,
                                           parse_response, parse_on_error, fail_fast,
                                           self.statsd_client, self.kafka_producer)


async def speculative_requests(request: asyncio.Task, speculative_request: asyncio.Task) -> RequestResult:
    done, pending = await asyncio.wait([request, speculative_request], return_when=asyncio.FIRST_COMPLETED)

    if request in done:
        speculative_request.cancel()
        return request.result()

    if speculative_request in done:
        if speculative_request.result() is not None:
            request.cancel()
            return speculative_request.result()

    await request
    return request.result()
