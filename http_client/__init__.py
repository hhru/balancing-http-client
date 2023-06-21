import abc
import asyncio
import contextvars
from typing import Optional
from asyncio import TimeoutError

import aiohttp
from aiohttp import ClientSession
from aiohttp.client_exceptions import ClientError

from http_client.options import options
from http_client.request_response import NoAvailableServerException, RequestBuilder, RequestResult
from http_client.util import make_body, make_mfd, to_unicode

_elapsed_time = contextvars.ContextVar('elapsed')


class RequestEngine(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def execute(self) -> 'RequestResult':
        raise NotImplementedError


class RequestEngineBuilder(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def build(self, request: RequestBuilder, profile, execute_request, modify_http_request_hook, debug_mode,
              parse_response, parse_on_error, fail_fast) -> RequestEngine:
        raise NotImplementedError


class HttpClient:
    def __init__(self, http_client_impl: ClientSession, source_app, request_engine_builder: RequestEngineBuilder, *,
                 modify_http_request_hook=None, debug_mode=False):
        self.http_client_impl = http_client_impl
        self.source_app = source_app
        self.debug_mode = debug_mode
        self.modify_http_request_hook = modify_http_request_hook
        self.request_engine_builder = request_engine_builder

    async def get_url(self, host, path, *, name=None, data=None, headers=None, follow_redirects=True, profile=None,
                      connect_timeout=None, request_timeout=None, max_timeout_tries=None,
                      parse_response=True, parse_on_error=False, fail_fast=False,
                      speculative_timeout_pct=None):

        request = RequestBuilder(
            host, self.source_app, path, name, 'GET', data, headers, None, None,
            connect_timeout, request_timeout, max_timeout_tries, speculative_timeout_pct, follow_redirects
        )

        request_engine = self.request_engine_builder.build(request, profile, self.execute_request,
                                                           self.modify_http_request_hook, self.debug_mode,
                                                           parse_response, parse_on_error, fail_fast)
        return await request_engine.execute()

    async def head_url(self, host, path, *, name=None, data=None, headers=None, follow_redirects=True, profile=None,
                       connect_timeout=None, request_timeout=None, max_timeout_tries=None,
                       fail_fast=False, speculative_timeout_pct=None):

        request = RequestBuilder(
            host, self.source_app, path, name, 'HEAD', data, headers, None, None,
            connect_timeout, request_timeout, max_timeout_tries, speculative_timeout_pct, follow_redirects
        )

        request_engine = self.request_engine_builder.build(request, profile, self.execute_request,
                                                           self.modify_http_request_hook, self.debug_mode,
                                                           False, False, fail_fast)
        return await request_engine.execute()

    async def post_url(self, host, path, *,
                       name=None, data='', headers=None, files=None, content_type=None, follow_redirects=True,
                       profile=None, connect_timeout=None, request_timeout=None, max_timeout_tries=None,
                       idempotent=False, parse_response=True, parse_on_error=False, fail_fast=False,
                       speculative_timeout_pct=None):

        request = RequestBuilder(
            host, self.source_app, path, name, 'POST', data, headers, files, content_type,
            connect_timeout, request_timeout, max_timeout_tries, speculative_timeout_pct, follow_redirects, idempotent
        )

        request_engine = self.request_engine_builder.build(request, profile, self.execute_request,
                                                           self.modify_http_request_hook, self.debug_mode,
                                                           parse_response, parse_on_error, fail_fast)
        return await request_engine.execute()

    async def put_url(self, host, path, *, name=None, data='', headers=None, content_type=None, follow_redirects=True,
                      profile=None, connect_timeout=None, request_timeout=None, max_timeout_tries=None, idempotent=True,
                      parse_response=True, parse_on_error=False, fail_fast=False,
                      speculative_timeout_pct=None):

        request = RequestBuilder(
            host, self.source_app, path, name, 'PUT', data, headers, None, content_type,
            connect_timeout, request_timeout, max_timeout_tries, speculative_timeout_pct, follow_redirects, idempotent
        )

        request_engine = self.request_engine_builder.build(request, profile, self.execute_request,
                                                           self.modify_http_request_hook, self.debug_mode,
                                                           parse_response, parse_on_error, fail_fast)
        return await request_engine.execute()

    async def delete_url(self, host, path, *, name=None, data=None, headers=None, content_type=None, profile=None,
                         connect_timeout=None, request_timeout=None, max_timeout_tries=None,
                         parse_response=True, parse_on_error=False, fail_fast=False,
                         speculative_timeout_pct=None):

        request = RequestBuilder(
            host, self.source_app, path, name, 'DELETE', data, headers, None, content_type,
            connect_timeout, request_timeout, max_timeout_tries, speculative_timeout_pct
        )

        request_engine = self.request_engine_builder.build(request, profile, self.execute_request,
                                                           self.modify_http_request_hook, self.debug_mode,
                                                           parse_response, parse_on_error, fail_fast)
        return await request_engine.execute()

    async def execute_request(self, request: RequestBuilder) -> RequestResult:
        _elapsed_time.set(0)
        try:
            response = await self.http_client_impl.request(
                method=request.method,
                url=request.url if request.url.startswith('http://') else f'http://{request.url}',
                headers=request.headers,
                data=request.body,
                allow_redirects=request.follow_redirects,
                timeout=request.timeout,
                proxy=request.proxy,
            )
            await response.read()
            return RequestResult(request, response, elapsed_time=_elapsed_time.get())
        except aiohttp.ClientConnectionError:
            raise
        except (ClientError, TimeoutError) as exc:
            return RequestResult(request, elapsed_time=_elapsed_time.get(), exc=exc)


class HttpClientFactory:
    def __init__(self, source_app, request_engine_builder: RequestEngineBuilder,
                 http_client: Optional[ClientSession] = None):
        if http_client is None:
            trace_config = aiohttp.TraceConfig()
            trace_config.on_request_start.append(self._on_request_start)
            trace_config.on_request_end.append(self._on_request_end)
            trace_config.on_request_exception.append(self._on_request_end)
            tcp_connector = aiohttp.TCPConnector(limit=options.max_clients)
            http_client = aiohttp.ClientSession(trace_configs=[trace_config], connector=tcp_connector)

        self.http_client = http_client
        self.source_app = source_app
        self.request_engine_builder = request_engine_builder

    @staticmethod
    async def _on_request_start(session, trace_config_ctx, params):
        trace_config_ctx.start = asyncio.get_event_loop().time()

    @staticmethod
    async def _on_request_end(session, trace_config_ctx, params):
        elapsed = asyncio.get_event_loop().time() - trace_config_ctx.start
        _elapsed_time.set(elapsed)

    def get_http_client(self, modify_http_request_hook=None, debug_mode=False) -> HttpClient:
        return HttpClient(
            self.http_client,
            self.source_app,
            self.request_engine_builder,
            modify_http_request_hook=modify_http_request_hook,
            debug_mode=debug_mode,
        )
