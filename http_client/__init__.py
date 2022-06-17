import asyncio
try:
    import ujson as json
except ImportError:
    import json
import re
import time
from asyncio import Future
from collections import OrderedDict
from dataclasses import dataclass
from functools import partial
from random import random, shuffle

import pycurl
import logging
from lxml import etree
from tornado.escape import to_unicode, utf8
from tornado.ioloop import IOLoop
from tornado.curl_httpclient import CurlAsyncHTTPClient
from tornado.httpclient import HTTPRequest, HTTPResponse, HTTPError
from tornado.httputil import HTTPHeaders

try:
    from tornado.stack_context import wrap
except ImportError:
    wrap = lambda f: f

from http_client.options import options

from http_client.util import make_url, make_body, make_mfd, response_from_debug

USER_AGENT_HEADER = 'User-Agent'


def HTTPResponse__repr__(self):
    repr_attrs = ['effective_url', 'code', 'reason', 'error']
    repr_values = [(attr, self.__dict__[attr]) for attr in repr_attrs]

    args = ', '.join(f'{name}={value}' for name, value in repr_values if value is not None)

    return f'{self.__class__.__name__}({args})'


HTTPResponse.__repr__ = HTTPResponse__repr__

http_client_logger = logging.getLogger('http_client')


class FailFastError(Exception):
    def __init__(self, failed_request: 'RequestResult'):
        self.failed_request = failed_request


class Server:

    def __init__(self, address, weight=1, rack=None, dc=None):
        self.address = address.rstrip('/')
        self.weight = int(weight)
        self.rack = rack
        self.datacenter = dc

        self.current_requests = 0
        self.requests = 0
        self.join_strategy = None

        if self.weight < 1:
            raise ValueError('weight should not be less then 1')

    def update(self, server):
        if self.weight != server.weight:
            ratio = float(server.weight) / float(self.weight)
            self.requests = int(self.requests * ratio)

        self.weight = server.weight
        self.rack = server.rack
        self.datacenter = server.datacenter

    def __str__(self) -> str:
        return f'{{address={self.address}, weight={self.weight}, dc={self.datacenter}}}'


class RetryPolicy:

    def __init__(self, properties):
        self.statuses = {}
        if properties:
            for status, config in properties.items():
                self.statuses[int(status)] = config.get('idempotent', 'false') == 'true'
        else:
            self.statuses = options.http_client_default_retry_policy

    def check_retry(self, response, idempotent):
        if response.code == 599:
            error = str(response.error)
            if error.startswith('HTTP 599: Failed to connect') or error.startswith('HTTP 599: Connection timed out'):
                return True

        if response.code not in self.statuses:
            return False

        return idempotent or self.statuses.get(response.code)


class Upstream:
    _single_host_upstream = None

    @classmethod
    def get_single_host_upstream(cls):
        if cls._single_host_upstream is not None:
            return cls._single_host_upstream

        cls._single_host_upstream = cls('single host upstream', {}, [Server('')])
        cls._single_host_upstream.balanced = False
        return cls._single_host_upstream

    def __init__(self, name, config, servers):
        self.name = name
        self.servers = []
        self.balanced = True
        self.max_tries = int(config.get('max_tries', options.http_client_default_max_tries))
        self.max_timeout_tries = int(config.get('max_timeout_tries', options.http_client_default_max_timeout_tries))
        self.connect_timeout = float(config.get('connect_timeout_sec', options.http_client_default_connect_timeout_sec))
        self.request_timeout = float(config.get('request_timeout_sec', options.http_client_default_request_timeout_sec))
        self.speculative_timeout = float(config.get('speculative_timeout_sec', 0))

        self.slow_start_interval = float(config.get('slow_start_interval_sec', 0))

        self.join_strategy = None
        if self.slow_start_interval != 0:
            self.join_strategy = DelayedSlowStartJoinStrategy(self.slow_start_interval)

        self.retry_policy = RetryPolicy(config.get('retry_policy', {}))
        trues = ('true', 'True', '1', True)
        self.session_required = config.get('session_required', options.http_client_default_session_required) in trues

        self._update_servers(servers)

    def borrow_server(self, exclude=None):
        min_index = None
        min_weights = None
        should_rescale_local_dc = True
        should_rescale_remote_dc = options.http_client_allow_cross_datacenter_requests

        if exclude is not None:
            tried_racks = {server.rack for server in self.servers if server is not None and server.address in exclude}
        else:
            tried_racks = None

        for index, server in enumerate(self.servers):
            if server is None:
                continue

            is_different_datacenter = server.datacenter != options.datacenter

            if is_different_datacenter and not options.http_client_allow_cross_datacenter_requests:
                continue

            should_rescale = server.requests >= server.weight
            if is_different_datacenter:
                should_rescale_remote_dc = should_rescale_remote_dc and should_rescale
            else:
                should_rescale_local_dc = should_rescale_local_dc and should_rescale

            groups = (is_different_datacenter, tried_racks is not None and server.rack in tried_racks)

            if server.join_strategy is not None and not server.join_strategy.can_handle_request(server):
                current_load = float('inf')
            else:
                current_load = server.current_requests / float(server.weight)

            load = server.requests / float(server.weight)

            weights = (groups, current_load, load)

            if (exclude is None or server.address not in exclude) and (min_index is None or weights < min_weights):
                min_weights = weights
                min_index = index

        if min_index is None:
            return None, None, None

        if should_rescale_local_dc or should_rescale_remote_dc:
            for server in self.servers:
                if server is not None:
                    is_same_dc = server.datacenter == options.datacenter
                    if (should_rescale_local_dc and is_same_dc) or (should_rescale_remote_dc and not is_same_dc):
                        server.requests -= server.weight

        server = self.servers[min_index]
        server.requests += 1
        server.current_requests += 1

        if server.join_strategy is not None:
            if server.join_strategy.is_complete():
                http_client_logger.info('slow start finished for %s upstream: %s', server.address, self.name)
                max_load = max(server.requests / float(server.weight) for server in self.servers
                               if server is not None)
                server.requests = int(server.weight * max_load)
                server.join_strategy = None
            else:
                http_client_logger.info('request to %s during slow start, upstream: %s', server.address, self.name)

        return server.address, server.rack, server.datacenter

    def return_server(self, host):
        server = next((server for server in self.servers if server is not None and server.address == host), None)
        if server is not None:
            if server.current_requests > 0:
                server.current_requests -= 1

    def update(self, upstream):
        servers = upstream.servers

        self.max_tries = upstream.max_tries
        self.max_timeout_tries = upstream.max_timeout_tries
        self.connect_timeout = upstream.connect_timeout
        self.request_timeout = upstream.request_timeout
        self.speculative_timeout = upstream.speculative_timeout

        self.slow_start_interval = upstream.slow_start_interval

        self.join_strategy = None
        if self.slow_start_interval != 0:
            self.join_strategy = DelayedSlowStartJoinStrategy(self.slow_start_interval)

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
        server.join_strategy = self.join_strategy
        for index, s in enumerate(self.servers):
            if s is None:
                self.servers[index] = server
                return

        self.servers.append(server)

    def __str__(self):
        return '[{}]'.format(','.join(server.address for server in self.servers if server is not None))


class DelayedSlowStartJoinStrategy:
    def __init__(self, slow_start_interval):
        self.initial_delay_end_time = time.time() + random() * slow_start_interval

    def is_complete(self):
        return time.time() > self.initial_delay_end_time

    def can_handle_request(self, server):
        if server.current_requests > 0:
            return False

        if time.time() < self.initial_delay_end_time:
            return False

        return True

    def __repr__(self):
        return (
            '<DelayedSlowStartJoinStrategy('
            f'initial_delay_end_time={self.initial_delay_end_time}'
            ')>'
        )


@dataclass(frozen=True)
class ResponseData:
    responseCode: int
    msg: str


class BalancedHttpRequest:
    def __init__(self, host: str, upstream: Upstream, source_app: str, uri: str, name: str,
                 method='GET', data=None, headers=None, files=None, content_type=None,
                 connect_timeout=None, request_timeout=None, max_timeout_tries=None,
                 speculative_timeout=None, follow_redirects=True, idempotent=True):
        self.source_app = source_app
        self.uri = uri if uri.startswith('/') else '/' + uri
        self.upstream = upstream
        self.name = name
        self.method = method
        self.connect_timeout = connect_timeout
        self.request_timeout = request_timeout
        self.follow_redirects = follow_redirects
        self.idempotent = idempotent
        self.speculative_timeout = speculative_timeout
        self.body = None

        if request_timeout is not None and max_timeout_tries is None:
            max_timeout_tries = options.http_client_default_max_timeout_tries

        if self.connect_timeout is None:
            self.connect_timeout = self.upstream.connect_timeout
        if self.request_timeout is None:
            self.request_timeout = self.upstream.request_timeout
        if max_timeout_tries is None:
            max_timeout_tries = self.upstream.max_timeout_tries
        if self.speculative_timeout is None:
            self.speculative_timeout = self.upstream.speculative_timeout

        self.session_required = self.upstream.session_required

        self.connect_timeout *= options.timeout_multiplier
        self.request_timeout *= options.timeout_multiplier

        self.headers = HTTPHeaders() if headers is None else HTTPHeaders(headers)
        if self.source_app and not self.headers.get(USER_AGENT_HEADER):
            self.headers[USER_AGENT_HEADER] = self.source_app
        if self.method == 'POST':
            if files:
                self.body, content_type = make_mfd(data, files)
            else:
                self.body = make_body(data)

            if content_type is None:
                content_type = self.headers.get('Content-Type', 'application/x-www-form-urlencoded')

            self.headers['Content-Length'] = str(len(self.body))
        elif self.method == 'PUT':
            self.body = make_body(data)
        else:
            self.uri = make_url(self.uri, **({} if data is None else data))

        if content_type is not None:
            self.headers['Content-Type'] = content_type
        self.request_time_left = self.request_timeout * max_timeout_tries
        self.tries = OrderedDict()
        self.balanced_host = host.rstrip('/')

    def make_request(self):
        host, rack, datacenter = None, None, None
        if self.upstream.balanced:
            host, rack, datacenter = self.upstream.borrow_server(self.tries.keys() if self.tries else None)

        request = HTTPRequest(
            url=(self.get_calling_address(self.get_host(host))) + self.uri,
            body=self.body,
            method=self.method,
            headers=self.headers,
            follow_redirects=self.follow_redirects,
            connect_timeout=self.connect_timeout,
            request_timeout=self.request_timeout,
        )
        request.current_host = host
        request.current_rack = rack
        request.current_datacenter = datacenter

        request.upstream_name = self.get_host_name()
        request.upstream_datacenter = datacenter

        if options.http_proxy_host is not None:
            request.proxy_host = options.http_proxy_host
            request.proxy_port = options.http_proxy_port

        return request

    @staticmethod
    def backend_available(host):
        return host is not None

    def get_calling_address(self, host):
        return host if self.backend_available(host) else self.upstream.name

    def get_host_name(self):
        return self.upstream.name if self.upstream.balanced else self.balanced_host

    def get_host(self, host):
        return host if self.upstream.balanced else self.balanced_host

    def check_retry(self, response):
        self.request_time_left -= response.request_time

        if self.upstream.balanced:
            do_retry = self.upstream.retry_policy.check_retry(response, self.idempotent)

            if response.request.current_host is not None:
                self.upstream.return_server(response.request.current_host)
        else:
            do_retry = False

        do_retry = do_retry and self.upstream.max_tries > len(self.tries) and self.request_time_left > 0
        return do_retry

    def check_speculative_retry(self):
        return self.idempotent and self.upstream.max_tries > len(self.tries)

    def enable_speculative_retry(self):
        return self.speculative_timeout != 0

    def register_try(self, response):
        host = self.get_host(response.request.current_host)
        self.tries[host] = ResponseData(response.code, str(response.error))

    @staticmethod
    def get_url(request, host):
        return f'http://{host}{request.uri}'

    @staticmethod
    def get_trace(request):
        def _get_server_address(host):
            if request.upstream.balanced:
                return host
            return request.balanced_host

        return ' -> '.join([f'{_get_server_address(host)}~{data.responseCode}~{data.msg}'
                            for host, data in request.tries.items()])


class HttpClientFactory:
    def __init__(self, source_app, tornado_http_client, *, upstreams=None, statsd_client=None, kafka_producer=None):
        self.tornado_http_client = tornado_http_client
        self.source_app = source_app
        self.statsd_client = statsd_client
        self.kafka_producer = kafka_producer
        self.upstreams = {} if upstreams is None else upstreams

    def get_http_client(self, modify_http_request_hook=None, debug_mode=False):
        return HttpClient(
            self.tornado_http_client,
            self.source_app,
            self.upstreams,
            statsd_client=self.statsd_client,
            kafka_producer=self.kafka_producer,
            modify_http_request_hook=modify_http_request_hook,
            debug_mode=debug_mode
        )

    def update_upstream(self, upstream):
        current_upstream = self.upstreams.get(upstream.name)

        if current_upstream is None:
            shuffle(upstream.servers)
            self.upstreams[upstream.name] = upstream
            http_client_logger.debug('add %s upstream: %s', upstream.name, str(upstream))
            return

        current_upstream.update(upstream)
        http_client_logger.debug('update %s upstream: %s', upstream.name, str(upstream))


class HttpClient:
    def __init__(self, http_client_impl, source_app, upstreams, *,
                 statsd_client=None, kafka_producer=None, modify_http_request_hook=None, debug_mode=False):
        self.http_client_impl = http_client_impl
        self.source_app = source_app
        self.debug_mode = debug_mode
        self.modify_http_request_hook = modify_http_request_hook
        self.upstreams = upstreams
        self.statsd_client = statsd_client
        self.kafka_producer = kafka_producer

    def get_upstream(self, host):
        return self.upstreams.get(host, Upstream.get_single_host_upstream())

    def get_url(self, host, uri, *, name=None, data=None, headers=None, follow_redirects=True,
                connect_timeout=None, request_timeout=None, max_timeout_tries=None,
                callback=None, parse_response=True, parse_on_error=False, fail_fast=False, speculative_timeout=None):

        request = BalancedHttpRequest(
            host, self.get_upstream(host), self.source_app, uri, name, 'GET', data, headers, None, None,
            connect_timeout, request_timeout, max_timeout_tries, speculative_timeout, follow_redirects
        )

        return self._fetch_with_retry(request, callback, parse_response, parse_on_error, fail_fast)

    def head_url(self, host, uri, *, name=None, data=None, headers=None, follow_redirects=True,
                 connect_timeout=None, request_timeout=None, max_timeout_tries=None,
                 callback=None, fail_fast=False, speculative_timeout=None):

        request = BalancedHttpRequest(
            host, self.get_upstream(host), self.source_app, uri, name, 'HEAD', data, headers, None, None,
            connect_timeout, request_timeout, max_timeout_tries, speculative_timeout, follow_redirects
        )

        return self._fetch_with_retry(request, callback, False, False, fail_fast)

    def post_url(self, host, uri, *,
                 name=None, data='', headers=None, files=None, content_type=None, follow_redirects=True,
                 connect_timeout=None, request_timeout=None, max_timeout_tries=None, idempotent=False,
                 callback=None, parse_response=True, parse_on_error=False, fail_fast=False, speculative_timeout=None):

        request = BalancedHttpRequest(
            host, self.get_upstream(host), self.source_app, uri, name, 'POST', data, headers, files, content_type,
            connect_timeout, request_timeout, max_timeout_tries, speculative_timeout, follow_redirects, idempotent
        )

        return self._fetch_with_retry(request, callback, parse_response, parse_on_error, fail_fast)

    def put_url(self, host, uri, *, name=None, data='', headers=None, content_type=None, follow_redirects=True,
                connect_timeout=None, request_timeout=None, max_timeout_tries=None, idempotent=True,
                callback=None, parse_response=True, parse_on_error=False, fail_fast=False, speculative_timeout=None):

        request = BalancedHttpRequest(
            host, self.get_upstream(host), self.source_app, uri, name, 'PUT', data, headers, None, content_type,
            connect_timeout, request_timeout, max_timeout_tries, speculative_timeout, follow_redirects, idempotent
        )

        return self._fetch_with_retry(request, callback, parse_response, parse_on_error, fail_fast)

    def delete_url(self, host, uri, *, name=None, data=None, headers=None, content_type=None,
                   connect_timeout=None, request_timeout=None, max_timeout_tries=None,
                   callback=None, parse_response=True, parse_on_error=False, fail_fast=False, speculative_timeout=None):

        request = BalancedHttpRequest(
            host, self.get_upstream(host), self.source_app, uri, name, 'DELETE', data, headers, None, content_type,
            connect_timeout, request_timeout, max_timeout_tries, speculative_timeout
        )

        return self._fetch_with_retry(request, callback, parse_response, parse_on_error, fail_fast)

    def _fetch_with_retry(self, balanced_request, callback, parse_response, parse_on_error,
                          fail_fast) -> 'Future[RequestResult]':
        future = Future()

        def request_finished_callback(response):
            if future.done():
                return

            if self.statsd_client is not None and len(balanced_request.tries) > 1:
                self.statsd_client.count(
                    'http.client.retries', 1,
                    upstream=balanced_request.get_host_name(),
                    dc=response.request.current_datacenter,
                    first_status=next(iter(balanced_request.tries.values())).responseCode,
                    tries=len(balanced_request.tries),
                    status=response.code
                )

            result = RequestResult(balanced_request, response, parse_response, parse_on_error)

            if callable(callback):
                wrap(callback)(result.data, result.response)

            if fail_fast and result.failed:
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

            request = response.request
            retries_count = len(balanced_request.tries)

            response, debug_extra = self._unwrap_debug(balanced_request, request, response, retries_count)
            balanced_request.register_try(response)
            do_retry = balanced_request.check_retry(response)

            self._log_response(balanced_request, response, retries_count, do_retry, debug_extra)

            if do_retry:
                IOLoop.current().add_future(self._fetch(balanced_request), retry_callback)
                return

            request_finished_callback(response)

        def speculative_retry():
            if future.done():
                return
            do_retry = balanced_request.check_speculative_retry()
            if do_retry:
                IOLoop.current().add_future(self._fetch(balanced_request), retry_callback)
                return

        if callable(self.modify_http_request_hook):
            self.modify_http_request_hook(balanced_request)

        IOLoop.current().add_future(self._fetch(balanced_request), retry_callback)
        if balanced_request.enable_speculative_retry():
            IOLoop.current().call_later(balanced_request.speculative_timeout, speculative_retry)

        return future

    def _fetch(self, balanced_request):
        request = balanced_request.make_request()

        if not balanced_request.backend_available(balanced_request.get_host(request.current_host)):
            future = Future()
            future.set_result(HTTPResponse(
                request, 502, error=HTTPError(502, 'No backend available for ' +
                                              balanced_request.get_host_name()),
                request_time=0
            ))
            return future

        if isinstance(self.http_client_impl, CurlAsyncHTTPClient):
            request.prepare_curl_callback = partial(
                self._prepare_curl_callback, next_callback=request.prepare_curl_callback
            )

        return self.http_client_impl.fetch(request)

    @staticmethod
    def _prepare_curl_callback(curl, next_callback):
        curl.setopt(pycurl.NOSIGNAL, 1)

        if callable(next_callback):
            next_callback(curl)

    def _unwrap_debug(self, balanced_request, request, response, retries_count):
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
                    '_rack': response.request.current_rack,
                    '_datacenter': response.request.current_datacenter,
                    '_balanced_request': balanced_request
                })
        except Exception:
            http_client_logger.exception('Cannot get response from debug')

        return response, debug_extra

    def _log_response(self, balanced_request, response, retries_count, do_retry, debug_extra):
        size = f' {len(response.body)} bytes' if response.body is not None else ''
        is_server_error = response.code >= 500
        request = response.request
        host = request.current_host
        if do_retry:
            retry = f' on retry {retries_count}' if retries_count > 0 else ''
            log_message = f'balanced_request_response: {response.code} got {size}{retry}, will retry ' \
                          f'{balanced_request.method} {response.effective_url} in {response.request_time * 1000:.2f}ms'
            log_method = http_client_logger.info if is_server_error else http_client_logger.debug
        else:
            msg_label = 'balanced_request_final_error' if is_server_error else 'balanced_request_final_response'
            log_message = f'{msg_label}: {response.code} got {size}' \
                          f'{balanced_request.method} ' \
                          f'{BalancedHttpRequest.get_url(balanced_request, balanced_request.get_host(host))}, ' \
                          f'trace: {BalancedHttpRequest.get_trace(balanced_request)}'

            log_method = http_client_logger.warning if is_server_error else http_client_logger.info

        log_method(log_message, extra=debug_extra)

        if response.code == 599:
            timings_info = (f'{stage}={timing * 1000:.3f}ms' for stage, timing in response.time_info.items())
            http_client_logger.info('Curl timings: %s', ' '.join(timings_info))

        if self.statsd_client is not None:
            self.statsd_client.stack()
            self.statsd_client.count(
                'http.client.requests', 1,
                upstream=balanced_request.get_host_name(),
                dc=request.current_datacenter,
                final='false' if do_retry else 'true',
                status=response.code
            )
            self.statsd_client.time(
                'http.client.request.time',
                int(response.request_time * 1000),
                dc=request.current_datacenter,
                upstream=balanced_request.get_host_name()
            )
            self.statsd_client.flush()

        if self.kafka_producer is not None and not do_retry:
            dc = request.current_datacenter or options.datacenter or 'unknown'
            current_host = host or 'unknown'
            request_id = balanced_request.headers.get('X-Request-Id', 'unknown')
            upstream = balanced_request.get_host_name() or 'unknown'

            asyncio.get_event_loop().create_task(self.kafka_producer.send(
                'metrics_requests',
                utf8(f'{{"app":"{options.app}","dc":"{dc}","hostname":"{current_host}","requestId":"{request_id}",'
                     f'"status":{response.code},"ts":{int(time.time())},"upstream":"{upstream}"}}')
            ))


class DataParseError:
    __slots__ = ('attrs',)

    def __init__(self, **attrs):
        self.attrs = attrs


class RequestResult:
    __slots__ = (
        'name', 'request', 'response', 'parse_on_error', 'parse_response', '_content_type', '_data', '_data_parse_error'
    )

    _args = ('request', 'response', 'parse_response', 'parse_on_error')

    def __init__(self, request: BalancedHttpRequest, response: HTTPResponse,
                 parse_response: bool, parse_on_error: bool):
        self.name = request.name
        self.request = request
        self.response = response
        self.parse_response = parse_response
        self.parse_on_error = parse_on_error

        self._content_type = None
        self._data = None
        self._data_parse_error = None

    def __repr__(self):
        args = ', '.join(f'{a}={repr(getattr(self, a))}' for a in self._args)
        return f'{self.__class__.__name__}({args})'

    def _parse_data(self):
        if self._data is not None or self._data_parse_error is not None:
            return

        if self.response.error and not self.parse_on_error:
            data_or_error = DataParseError(reason=str(self.response.error), code=self.response.code)
        elif not self.parse_response or self.response.code == 204:
            data_or_error = self.response.body
            self._content_type = 'raw'
        else:
            data_or_error = None
            content_type = self.response.headers.get('Content-Type', '')
            for name, (regex, parser) in RESPONSE_CONTENT_TYPES.items():
                if regex.search(content_type):
                    data_or_error = parser(self.response)
                    self._content_type = name
                    break

        if isinstance(data_or_error, DataParseError):
            self._data_parse_error = data_or_error
        else:
            self._data = data_or_error

    @property
    def data(self):
        self._parse_data()
        return self._data

    @property
    def data_parsing_failed(self) -> bool:
        self._parse_data()
        return self._data_parse_error is not None

    @property
    def failed(self):
        return self.response.error or self.data_parsing_failed

    def to_dict(self):
        self._parse_data()

        if isinstance(self._data_parse_error, DataParseError):
            return {
                'error': {k: v for k, v in self._data_parse_error.attrs.items()}
            }

        return self.data if self._content_type == 'json' else None

    def to_etree_element(self):
        self._parse_data()

        if isinstance(self._data_parse_error, DataParseError):
            return etree.Element('error', **{k: str(v) for k, v in self._data_parse_error.attrs.items()})

        return self.data if self._content_type == 'xml' else None


def _parse_response(response, parser, response_type):
    try:
        return parser(response.body)
    except Exception:
        _preview_len = 100

        if response.body is None:
            body_preview = None
        elif len(response.body) > _preview_len:
            body_preview = response.body[:_preview_len]
        else:
            body_preview = response.body

        if body_preview is not None:
            try:
                body_preview = f'excerpt: {to_unicode(body_preview)}'
            except Exception:
                body_preview = f'could not be converted to unicode, excerpt: {str(body_preview)}'
        else:
            body_preview = 'is None'

        http_client_logger.exception(
            'failed to parse %s response from %s, body %s',
            response_type, response.effective_url, body_preview
        )

        return DataParseError(reason=f'invalid {response_type}')


_xml_parser = etree.XMLParser(strip_cdata=False)
_parse_response_xml = partial(
    _parse_response, parser=lambda body: etree.fromstring(body, parser=_xml_parser), response_type='xml'
)

_parse_response_json = partial(_parse_response, parser=json.loads, response_type='json')

_parse_response_text = partial(_parse_response, parser=to_unicode, response_type='text')

RESPONSE_CONTENT_TYPES = {
    'xml': (re.compile('.*xml.?'), _parse_response_xml),
    'json': (re.compile('.*json.?'), _parse_response_json),
    'text': (re.compile('.*text/plain.?'), _parse_response_text),
}
