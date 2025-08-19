import abc
import asyncio
import contextvars
import time
from asyncio import TimeoutError

import aiohttp
from aiohttp.client_exceptions import ClientConnectorError, ClientError, ServerTimeoutError

from http_client.options import options
from http_client.request_response import (
    CLIENT_ERROR,
    DEADLINE_TIMEOUT_MS_HEADER,
    INSUFFICIENT_TIMEOUT,
    OUTER_TIMEOUT_MS_HEADER,
    SERVER_TIMEOUT,
    RequestBuilder,
    RequestResult,
)
from http_client.util import set_contextvar

current_client_request = contextvars.ContextVar('current_client_request')
current_client_request_status = contextvars.ContextVar('current_client_request_status')
extra_client_params = contextvars.ContextVar('extra_client_params', default=(None, False))


class RequestEngine(abc.ABC):
    @abc.abstractmethod
    async def execute(self) -> RequestResult:
        raise NotImplementedError


class RequestEngineBuilder(abc.ABC):
    @abc.abstractmethod
    def build(
        self,
        request: RequestBuilder,
        profile,
        execute_request,
        modify_http_request_hook,
        debug_enabled,
        parse_response,
        parse_on_error,
        fail_fast,
    ) -> RequestEngine:
        raise NotImplementedError


class HttpClient:
    def __init__(self, http_client_impl, source_app, request_engine_builder: RequestEngineBuilder):
        self.http_client_impl = http_client_impl
        self.source_app = source_app
        self.request_engine_builder = request_engine_builder

    async def get_url(
        self,
        host,
        path,
        *,
        name=None,
        data=None,
        headers=None,
        follow_redirects=True,
        profile=None,
        connect_timeout=None,
        request_timeout=None,
        max_timeout_tries=None,
        parse_response=True,
        parse_on_error=True,
        fail_fast=False,
        speculative_timeout_pct=None,
    ) -> RequestResult:
        modify_http_request_hook, debug_enabled = extra_client_params.get()

        request = RequestBuilder(
            host,
            self.source_app,
            path,
            name,
            'GET',
            data,
            headers,
            None,
            None,
            connect_timeout,
            request_timeout,
            max_timeout_tries,
            speculative_timeout_pct,
            follow_redirects,
        )

        request_engine = self.request_engine_builder.build(
            request,
            profile,
            self.execute_request,
            modify_http_request_hook,
            debug_enabled,
            parse_response,
            parse_on_error,
            fail_fast,
        )
        return await request_engine.execute()

    async def head_url(
        self,
        host,
        path,
        *,
        name=None,
        data=None,
        headers=None,
        follow_redirects=True,
        profile=None,
        connect_timeout=None,
        request_timeout=None,
        max_timeout_tries=None,
        fail_fast=False,
        speculative_timeout_pct=None,
    ) -> RequestResult:
        modify_http_request_hook, debug_enabled = extra_client_params.get()

        request = RequestBuilder(
            host,
            self.source_app,
            path,
            name,
            'HEAD',
            data,
            headers,
            None,
            None,
            connect_timeout,
            request_timeout,
            max_timeout_tries,
            speculative_timeout_pct,
            follow_redirects,
        )

        request_engine = self.request_engine_builder.build(
            request, profile, self.execute_request, modify_http_request_hook, debug_enabled, False, False, fail_fast
        )
        return await request_engine.execute()

    async def post_url(
        self,
        host,
        path,
        *,
        name=None,
        data='',
        headers=None,
        files=None,
        content_type=None,
        follow_redirects=True,
        profile=None,
        connect_timeout=None,
        request_timeout=None,
        max_timeout_tries=None,
        idempotent=False,
        parse_response=True,
        parse_on_error=True,
        fail_fast=False,
        speculative_timeout_pct=None,
        use_form_data=False,
    ) -> RequestResult:
        modify_http_request_hook, debug_enabled = extra_client_params.get()

        request = RequestBuilder(
            host,
            self.source_app,
            path,
            name,
            'POST',
            data,
            headers,
            files,
            content_type,
            connect_timeout,
            request_timeout,
            max_timeout_tries,
            speculative_timeout_pct,
            follow_redirects,
            idempotent,
            use_form_data,
        )

        request_engine = self.request_engine_builder.build(
            request,
            profile,
            self.execute_request,
            modify_http_request_hook,
            debug_enabled,
            parse_response,
            parse_on_error,
            fail_fast,
        )
        return await request_engine.execute()

    async def put_url(
        self,
        host,
        path,
        *,
        name=None,
        data='',
        headers=None,
        content_type=None,
        follow_redirects=True,
        profile=None,
        connect_timeout=None,
        request_timeout=None,
        max_timeout_tries=None,
        idempotent=True,
        parse_response=True,
        parse_on_error=True,
        fail_fast=False,
        speculative_timeout_pct=None,
    ) -> RequestResult:
        modify_http_request_hook, debug_enabled = extra_client_params.get()

        request = RequestBuilder(
            host,
            self.source_app,
            path,
            name,
            'PUT',
            data,
            headers,
            None,
            content_type,
            connect_timeout,
            request_timeout,
            max_timeout_tries,
            speculative_timeout_pct,
            follow_redirects,
            idempotent,
        )

        request_engine = self.request_engine_builder.build(
            request,
            profile,
            self.execute_request,
            modify_http_request_hook,
            debug_enabled,
            parse_response,
            parse_on_error,
            fail_fast,
        )
        return await request_engine.execute()

    async def delete_url(
        self,
        host,
        path,
        *,
        name=None,
        data=None,
        headers=None,
        content_type=None,
        profile=None,
        connect_timeout=None,
        request_timeout=None,
        max_timeout_tries=None,
        parse_response=True,
        parse_on_error=True,
        fail_fast=False,
        speculative_timeout_pct=None,
    ) -> RequestResult:
        modify_http_request_hook, debug_enabled = extra_client_params.get()

        request = RequestBuilder(
            host,
            self.source_app,
            path,
            name,
            'DELETE',
            data,
            headers,
            None,
            content_type,
            connect_timeout,
            request_timeout,
            max_timeout_tries,
            speculative_timeout_pct,
        )

        request_engine = self.request_engine_builder.build(
            request,
            profile,
            self.execute_request,
            modify_http_request_hook,
            debug_enabled,
            parse_response,
            parse_on_error,
            fail_fast,
        )
        return await request_engine.execute()

    async def execute_request(self, request: RequestBuilder) -> RequestResult:
        return await self.http_client_impl.fetch(request)


class AIOHttpClientWrapper:
    def __init__(self):
        self._elapsed_time = contextvars.ContextVar('elapsed_time')
        self._start_time = contextvars.ContextVar('start_time')
        trace_config = aiohttp.TraceConfig()
        trace_config.on_request_start.append(self._on_request_start)
        trace_config.on_request_end.append(self._on_request_end)
        trace_config.on_request_exception.append(self._on_request_exception)
        tcp_connector = aiohttp.TCPConnector(limit=options.max_clients)
        self.client_session = aiohttp.ClientSession(trace_configs=[trace_config], connector=tcp_connector)

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

        if isinstance(params.exception, (ClientConnectorError, ServerTimeoutError)):
            current_client_request_status.set(CLIENT_ERROR)

        elif isinstance(params.exception, TimeoutError):
            deadline_timeout = params.headers.get(DEADLINE_TIMEOUT_MS_HEADER)
            outer_timeout = params.headers.get(OUTER_TIMEOUT_MS_HEADER)

            has_insufficient_timeout = (
                deadline_timeout is not None
                and outer_timeout is not None
                and int(deadline_timeout) < int(outer_timeout)
            )
            status = INSUFFICIENT_TIMEOUT if has_insufficient_timeout else SERVER_TIMEOUT
            current_client_request_status.set(status)

        elif isinstance(params.exception, ClientError):
            current_client_request_status.set(CLIENT_ERROR)

    def close(self):
        pass

    async def fetch(self, request: RequestBuilder) -> RequestResult:
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
                response_streaming = 'text/event-stream' in response.headers.get('Content-Type', {})
                if response_streaming:
                    response_body = None
                else:
                    response_body = await response.read()
                result = RequestResult(
                    request,
                    response.status,
                    response,
                    response_body,
                    response_streaming=response_streaming,
                    elapsed_time=self._elapsed_time.get(),
                )

            except (ClientError, TimeoutError) as exc:
                result = RequestResult(
                    request,
                    current_client_request_status.get() or CLIENT_ERROR,
                    elapsed_time=self._elapsed_time.get(),
                    exc=exc,
                )

        return result


class HttpClientFactory:
    def __init__(self, source_app, request_engine_builder: RequestEngineBuilder):
        self.http_client = AIOHttpClientWrapper()
        self.source_app = source_app
        self.request_engine_builder = request_engine_builder

    def get_http_client(self) -> HttpClient:
        return HttpClient(self.http_client, self.source_app, self.request_engine_builder)
