import abc
import asyncio
import contextvars
from asyncio import Future, TimeoutError
import time
from typing import Callable

import aiohttp
import yarl
from aiohttp.client_exceptions import ClientError

from http_client.options import options
from http_client.request_response import RequestBuilder, RequestResult, TornadoResponseWrapper
from http_client.util import set_contextvar

current_client_request = contextvars.ContextVar('current_client_request')
current_client_request_status = contextvars.ContextVar('current_client_request_status')
extra_client_params = contextvars.ContextVar('extra_client_params', default=(None, False))


class RequestEngine(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def execute(self) -> Future[RequestResult]:
        raise NotImplementedError


class RequestEngineBuilder(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def build(self, request: RequestBuilder, profile, execute_request, modify_http_request_hook, debug_enabled,
              parse_response, parse_on_error, fail_fast) -> RequestEngine:
        raise NotImplementedError


class HttpClient:
    def __init__(self, http_client_impl, source_app, request_engine_builder: RequestEngineBuilder):
        self.http_client_impl = http_client_impl
        self.source_app = source_app
        self.request_engine_builder = request_engine_builder

    def get_url(self, host, path, *, name=None, data=None, headers=None, follow_redirects=True, profile=None,
                connect_timeout=None, request_timeout=None, max_timeout_tries=None,
                parse_response=True, parse_on_error=True, fail_fast=False,
                speculative_timeout_pct=None) -> Future[RequestResult]:
        modify_http_request_hook, debug_enabled = extra_client_params.get()

        request = RequestBuilder(
            host, self.source_app, path, name, 'GET', data, headers, None, None,
            connect_timeout, request_timeout, max_timeout_tries, speculative_timeout_pct, follow_redirects
        )

        request_engine = self.request_engine_builder.build(request, profile, self.execute_request,
                                                           modify_http_request_hook, debug_enabled,
                                                           parse_response, parse_on_error, fail_fast)
        return request_engine.execute()

    def head_url(self, host, path, *, name=None, data=None, headers=None, follow_redirects=True, profile=None,
                 connect_timeout=None, request_timeout=None, max_timeout_tries=None,
                 fail_fast=False, speculative_timeout_pct=None) -> Future[RequestResult]:
        modify_http_request_hook, debug_enabled = extra_client_params.get()

        request = RequestBuilder(
            host, self.source_app, path, name, 'HEAD', data, headers, None, None,
            connect_timeout, request_timeout, max_timeout_tries, speculative_timeout_pct, follow_redirects
        )

        request_engine = self.request_engine_builder.build(request, profile, self.execute_request,
                                                           modify_http_request_hook, debug_enabled,
                                                           False, False, fail_fast)
        return request_engine.execute()

    def post_url(self, host, path, *,
                 name=None, data='', headers=None, files=None, content_type=None, follow_redirects=True, profile=None,
                 connect_timeout=None, request_timeout=None, max_timeout_tries=None, idempotent=False,
                 parse_response=True, parse_on_error=True, fail_fast=False,
                 speculative_timeout_pct=None, use_form_data=False) -> Future[RequestResult]:
        modify_http_request_hook, debug_enabled = extra_client_params.get()

        request = RequestBuilder(
            host, self.source_app, path, name, 'POST', data, headers, files, content_type, connect_timeout,
            request_timeout, max_timeout_tries, speculative_timeout_pct, follow_redirects, idempotent, use_form_data
        )

        request_engine = self.request_engine_builder.build(request, profile, self.execute_request,
                                                           modify_http_request_hook, debug_enabled,
                                                           parse_response, parse_on_error, fail_fast)
        return request_engine.execute()

    def put_url(self, host, path, *, name=None, data='', headers=None, content_type=None, follow_redirects=True,
                profile=None, connect_timeout=None, request_timeout=None, max_timeout_tries=None, idempotent=True,
                parse_response=True, parse_on_error=True, fail_fast=False,
                speculative_timeout_pct=None) -> Future[RequestResult]:
        modify_http_request_hook, debug_enabled = extra_client_params.get()

        request = RequestBuilder(
            host, self.source_app, path, name, 'PUT', data, headers, None, content_type,
            connect_timeout, request_timeout, max_timeout_tries, speculative_timeout_pct, follow_redirects, idempotent
        )

        request_engine = self.request_engine_builder.build(request, profile, self.execute_request,
                                                           modify_http_request_hook, debug_enabled,
                                                           parse_response, parse_on_error, fail_fast)
        return request_engine.execute()

    def delete_url(self, host, path, *, name=None, data=None, headers=None, content_type=None, profile=None,
                   connect_timeout=None, request_timeout=None, max_timeout_tries=None,
                   parse_response=True, parse_on_error=True, fail_fast=False,
                   speculative_timeout_pct=None) -> Future[RequestResult]:
        modify_http_request_hook, debug_enabled = extra_client_params.get()

        request = RequestBuilder(
            host, self.source_app, path, name, 'DELETE', data, headers, None, content_type,
            connect_timeout, request_timeout, max_timeout_tries, speculative_timeout_pct
        )

        request_engine = self.request_engine_builder.build(request, profile, self.execute_request,
                                                           modify_http_request_hook, debug_enabled,
                                                           parse_response, parse_on_error, fail_fast)
        return request_engine.execute()

    def execute_request(self, request: RequestBuilder) -> Future[RequestResult]:
        return self.http_client_impl.fetch(request)


class AIOHttpClientWrapper:
    """
    wrapper of aiohttp.ClientSession

    while we heavily dependent on tornado_mocks, we must abide tornado client interface
    """
    def __init__(self):
        self._elapsed_time = contextvars.ContextVar('elapsed_time')
        self._start_time = contextvars.ContextVar('start_time')
        trace_config = aiohttp.TraceConfig()
        trace_config.on_request_start.append(self._on_request_start)
        trace_config.on_request_end.append(self._on_request_end)
        trace_config.on_request_exception.append(self._on_request_exception)
        tcp_connector = aiohttp.TCPConnector(limit=options.max_clients)
        self.client_session = aiohttp.ClientSession(trace_configs=[trace_config], connector=tcp_connector)

        class IoLoopTestWrapper:
            """
            only for testing
            """
            @staticmethod
            def add_callback(func):
                loop = asyncio.get_event_loop()
                loop.call_soon(func)

        self.io_loop = IoLoopTestWrapper()

    async def _on_request_start(self, session, trace_config_ctx, params):
        self._start_time.set(time.time())
        trace_config_ctx.start = asyncio.get_event_loop().time()

    async def _on_request_end(self, session, trace_config_ctx, params):
        elapsed = asyncio.get_event_loop().time() - trace_config_ctx.start
        self._elapsed_time.set(elapsed)
        current_client_request_status.set(params.response.status)

    async def _on_request_exception(self, session, trace_config_ctx, params):
        elapsed = asyncio.get_event_loop().time() - trace_config_ctx.start
        self._elapsed_time.set(elapsed)
        if isinstance(params.exception, (ClientError, TimeoutError)):
            current_client_request_status.set(599)

    def close(self):
        pass

    def fetch(self, request: RequestBuilder, raise_error=True, **kwargs) -> Future[RequestResult]:
        future = Future()

        def handle_response(response) -> None:
            if future.done():
                return

            if not isinstance(response, RequestResult):
                """
                only for testing
                tornado_mocks gives tornado.httpclient.HTTPResponse
                """
                resp = TornadoResponseWrapper(response)
                result = RequestResult(request, resp.status, resp, resp.body, elapsed_time=request.request_timeout)
                future.set_result(result)
            else:
                future.set_result(response)

        if isinstance(request, str):
            """
            only for testing
            """
            url = yarl.URL(request)
            host = f'{url.host}:{url.port}'
            path = url.raw_path_qs
            request = RequestBuilder(host, 'test', path, 'test_request', **kwargs)
        self.fetch_impl(request, handle_response)
        return future

    def fetch_impl(self, request: RequestBuilder, callback: Callable[[RequestResult], None]):
        async def real_fetch():
            with (
                set_contextvar(current_client_request, request),
                set_contextvar(current_client_request_status, None),
                set_contextvar(self._elapsed_time, 0),
                set_contextvar(self._start_time, 0),
            ):
                try:
                    response = await self.client_session.request(
                        method=request.method,
                        url=request.url,
                        headers=request.headers,
                        data=request.body,
                        allow_redirects=request.follow_redirects,
                        timeout=request.timeout,
                        proxy=request.proxy,
                    )
                    request.start_time = self._start_time.get()
                    response_body = await response.read()
                    result = RequestResult(
                        request, response.status, response, response_body, elapsed_time=self._elapsed_time.get()
                    )

                except (ClientError, TimeoutError) as exc:
                    result = RequestResult(
                        request,
                        current_client_request_status.get() or 599,
                        elapsed_time=self._elapsed_time.get(),
                        exc=exc,
                    )

            if callback is not None:
                callback(result)

            return result

        task = asyncio.create_task(real_fetch())
        return task


class HttpClientFactory:
    def __init__(self, source_app, request_engine_builder: RequestEngineBuilder):
        self.http_client = AIOHttpClientWrapper()
        self.source_app = source_app
        self.request_engine_builder = request_engine_builder

    def get_http_client(self) -> HttpClient:
        return HttpClient(self.http_client, self.source_app, self.request_engine_builder)
