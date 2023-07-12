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
from http_client.request_response import (NoAvailableServerException,
                                          RequestBuilder, RequestResult,
                                          TornadoResponseWrapper)
from http_client.util import make_body, make_mfd, to_unicode


class RequestEngine(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def execute(self) -> Future[RequestResult]:
        raise NotImplementedError


class RequestEngineBuilder(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def build(self, request: RequestBuilder, profile, execute_request, modify_http_request_hook, debug_mode,
              parse_response, parse_on_error, fail_fast) -> RequestEngine:
        raise NotImplementedError


class HttpClient:
    def __init__(self, http_client_impl, source_app, request_engine_builder: RequestEngineBuilder, *,
                 modify_http_request_hook=None, debug_mode=False):
        self.http_client_impl = http_client_impl
        self.source_app = source_app
        self.debug_mode = debug_mode
        self.modify_http_request_hook = modify_http_request_hook
        self.request_engine_builder = request_engine_builder

    def get_url(self, host, path, *, name=None, data=None, headers=None, follow_redirects=True, profile=None,
                connect_timeout=None, request_timeout=None, max_timeout_tries=None,
                parse_response=True, parse_on_error=False, fail_fast=False,
                speculative_timeout_pct=None) -> Future[RequestResult]:

        request = RequestBuilder(
            host, self.source_app, path, name, 'GET', data, headers, None, None,
            connect_timeout, request_timeout, max_timeout_tries, speculative_timeout_pct, follow_redirects
        )

        request_engine = self.request_engine_builder.build(request, profile, self.execute_request,
                                                           self.modify_http_request_hook, self.debug_mode,
                                                           parse_response, parse_on_error, fail_fast)
        return request_engine.execute()

    def head_url(self, host, path, *, name=None, data=None, headers=None, follow_redirects=True, profile=None,
                 connect_timeout=None, request_timeout=None, max_timeout_tries=None,
                 fail_fast=False, speculative_timeout_pct=None) -> Future[RequestResult]:

        request = RequestBuilder(
            host, self.source_app, path, name, 'HEAD', data, headers, None, None,
            connect_timeout, request_timeout, max_timeout_tries, speculative_timeout_pct, follow_redirects
        )

        request_engine = self.request_engine_builder.build(request, profile, self.execute_request,
                                                           self.modify_http_request_hook, self.debug_mode,
                                                           False, False, fail_fast)
        return request_engine.execute()

    def post_url(self, host, path, *,
                 name=None, data='', headers=None, files=None, content_type=None, follow_redirects=True, profile=None,
                 connect_timeout=None, request_timeout=None, max_timeout_tries=None, idempotent=False,
                 parse_response=True, parse_on_error=False, fail_fast=False,
                 speculative_timeout_pct=None) -> Future[RequestResult]:

        request = RequestBuilder(
            host, self.source_app, path, name, 'POST', data, headers, files, content_type,
            connect_timeout, request_timeout, max_timeout_tries, speculative_timeout_pct, follow_redirects, idempotent
        )

        request_engine = self.request_engine_builder.build(request, profile, self.execute_request,
                                                           self.modify_http_request_hook, self.debug_mode,
                                                           parse_response, parse_on_error, fail_fast)
        return request_engine.execute()

    def put_url(self, host, path, *, name=None, data='', headers=None, content_type=None, follow_redirects=True,
                profile=None, connect_timeout=None, request_timeout=None, max_timeout_tries=None, idempotent=True,
                parse_response=True, parse_on_error=False, fail_fast=False,
                speculative_timeout_pct=None) -> Future[RequestResult]:

        request = RequestBuilder(
            host, self.source_app, path, name, 'PUT', data, headers, None, content_type,
            connect_timeout, request_timeout, max_timeout_tries, speculative_timeout_pct, follow_redirects, idempotent
        )

        request_engine = self.request_engine_builder.build(request, profile, self.execute_request,
                                                           self.modify_http_request_hook, self.debug_mode,
                                                           parse_response, parse_on_error, fail_fast)
        return request_engine.execute()

    def delete_url(self, host, path, *, name=None, data=None, headers=None, content_type=None, profile=None,
                   connect_timeout=None, request_timeout=None, max_timeout_tries=None,
                   parse_response=True, parse_on_error=False, fail_fast=False,
                   speculative_timeout_pct=None) -> Future[RequestResult]:

        request = RequestBuilder(
            host, self.source_app, path, name, 'DELETE', data, headers, None, content_type,
            connect_timeout, request_timeout, max_timeout_tries, speculative_timeout_pct
        )

        request_engine = self.request_engine_builder.build(request, profile, self.execute_request,
                                                           self.modify_http_request_hook, self.debug_mode,
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
        trace_config.on_request_exception.append(self._on_request_end)
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

    def close(self):
        pass

    def fetch(self, request: RequestBuilder, raise_error=True, **kwargs) -> Future[RequestResult]:
        future = Future()

        def handle_response(response) -> None:
            if not isinstance(response, RequestResult):
                """
                only for testing
                tornado_mocks gives tornado.httpclient.HTTPResponse
                """
                result = RequestResult(request, TornadoResponseWrapper(response), elapsed_time=request.request_timeout)
                future.set_result(result)
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
            self._elapsed_time.set(0)
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
                await response.read()
                result = RequestResult(request, response, elapsed_time=self._elapsed_time.get())

            except (ClientError, TimeoutError) as exc:
                result = RequestResult(request, elapsed_time=self._elapsed_time.get(), exc=exc)

            if callback is not None:
                callback(result)

            return result

        task = asyncio.create_task(real_fetch())
        return task


class HttpClientFactory:
    def __init__(self, source_app, http_client: AIOHttpClientWrapper, request_engine_builder: RequestEngineBuilder):
        self.http_client = http_client
        self.source_app = source_app
        self.request_engine_builder = request_engine_builder

    def get_http_client(self, modify_http_request_hook=None, debug_mode=False) -> HttpClient:
        return HttpClient(
            self.http_client,
            self.source_app,
            self.request_engine_builder,
            modify_http_request_hook=modify_http_request_hook,
            debug_mode=debug_mode
        )
