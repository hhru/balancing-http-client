from __future__ import annotations

import copy
import inspect
from collections import namedtuple
from collections.abc import AsyncGenerator, Callable
from dataclasses import dataclass
from re import Pattern
from typing import Any, Optional, Union
from unittest.mock import Mock, patch
from urllib.parse import parse_qsl, urlencode

from aiohttp import ClientConnectionError, ClientResponse, ClientSession, StreamReader, hdrs, http
from aiohttp.client_proto import ResponseHandler
from aiohttp.helpers import TimerNoop
from multidict import CIMultiDict, CIMultiDictProxy, MultiDict
from typing_extensions import Self
from yarl import URL

from http_client.util import utf8

try:
    from aiohttp import RequestInfo
except ImportError:

    class RequestInfo:
        __slots__ = ('headers', 'method', 'real_url', 'url')

        def __init__(self, url: URL, method: str, headers: dict[str, str], real_url: str) -> None:
            self.url = url
            self.method = method
            self.headers = headers
            self.real_url = real_url


RequestCall = namedtuple('RequestCall', ['args', 'kwargs'])


@dataclass
class ProxyRequest:
    url: URL
    body: bytes
    method: str


class MockedRequest:
    def __init__(
        self,
        url: Union[URL, str, Pattern],
        method: str = hdrs.METH_GET,
        status: int = 200,
        body: str | bytes | AsyncGenerator[bytes, None] = '',
        exception: Optional[Exception] = None,
        headers: dict[str, str] | None = None,
        repeat: bool = False,
        response_function: Callable = None,
    ):
        self.url_or_pattern: Union[URL, Pattern]
        if isinstance(url, Pattern):
            self.url_or_pattern = url
        else:
            self.url_or_pattern = normalize_url(url)

        self.method = method.lower()
        self.status = status
        self.body = self.get_body(body)
        self.exception = exception
        self.response_function = response_function
        self.headers = headers
        self.repeat = repeat
        try:
            self.reason = http.RESPONSES[self.status][0]
        except (IndexError, KeyError):
            self.reason = ''

    @staticmethod
    def get_body(body: bytes | str | AsyncGenerator[bytes, None]) -> bytes | AsyncGenerator[bytes, None] | None:
        if isinstance(body, (bytes, AsyncGenerator)):
            return body
        elif isinstance(body, str):
            return str.encode(body)
        return None

    def match(self, method: str, url: URL) -> bool:
        if self.method != method.lower():
            return False

        if isinstance(url, Pattern):
            return self.match_regexp(url)
        else:
            return self.match_str(url)

    def match_str(self, url: URL) -> bool:
        if self.url_or_pattern.scheme != url.scheme:
            return False

        if self.url_or_pattern.host != url.host:
            return False

        if self.url_or_pattern.path.rstrip('/') != url.path.rstrip('/'):
            return False

        for mock_param, mock_value in self.url_or_pattern.query.items():
            if mock_param not in url.query or mock_value not in url.query.getall(mock_param):
                return False

        return True

    def match_regexp(self, url_pattern: Pattern) -> bool:
        assert isinstance(self.url_or_pattern, Pattern)
        return bool(self.url_or_pattern.match(str(url_pattern)))

    async def build_response(self, url: URL, **kwargs) -> Union[ClientResponse, Exception]:
        request_headers = kwargs.get('headers')

        if self.exception is not None:
            return self.exception

        if self.response_function is not None:
            url = url
            body = kwargs.get('data')
            return self.response_function(ProxyRequest(url, body, self.method.upper()))

        if request_headers is None:
            request_headers = {}
        request_kwargs = {}  # type: dict[str, Any]

        loop = Mock()
        loop.get_debug = Mock()
        loop.get_debug.return_value = True
        request_kwargs['request_info'] = RequestInfo(
            url=url,
            method=self.method,
            headers=CIMultiDictProxy(CIMultiDict(**request_headers)),
        )
        request_kwargs['writer'] = None
        request_kwargs['continue100'] = None
        request_kwargs['timer'] = TimerNoop()
        request_kwargs['traces'] = []
        request_kwargs['loop'] = loop
        request_kwargs['session'] = None

        _headers = CIMultiDict({})
        if self.headers:
            _headers.update(self.headers)
        raw_headers = tuple([(k.encode('utf8'), v.encode('utf8')) for k, v in _headers.items()])
        resp = ClientResponse(self.method, url, **request_kwargs)

        for hdr in _headers.getall(hdrs.SET_COOKIE, ()):
            resp.cookies.load(hdr)

        resp._headers = _headers
        resp._raw_headers = raw_headers
        resp.status = self.status
        resp.reason = self.reason
        resp.content = StreamReader(ResponseHandler(loop=loop), limit=2**16, loop=loop)
        if isinstance(self.body, AsyncGenerator):
            async for chunk in self.body:
                resp.content.feed_data(chunk)
        else:
            resp.content.feed_data(self.body)
        resp.content.feed_eof()
        return resp


class MockHttpClient:
    def __init__(self, passthrough: list[str] | None = None) -> None:
        self._passthrough = passthrough if passthrough is not None else []
        self.patcher = patch('aiohttp.client.ClientSession._request', side_effect=self._request_mock, autospec=True)
        self.requests: dict[Any, Any] = {}

    def __enter__(self) -> Self:
        self._responses: list[ClientResponse] = []
        self._matches: list[MockedRequest] = []
        self.patcher.start()
        self.patcher.return_value = self._request_mock
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        for response in self._responses:
            response.close()
        self.patcher.stop()
        self._responses.clear()
        self._matches.clear()

    def add(
        self,
        url: Union[URL, str, Pattern],
        method: str = hdrs.METH_GET,
        status: int = 200,
        body: str | bytes | AsyncGenerator[bytes, None] = '',
        exception: Exception | None = None,
        headers: dict[str, str] | None = None,
        repeat: bool = False,
        response_function=None,
    ) -> None:
        self._matches.insert(
            0,
            MockedRequest(
                url,
                method=method,
                status=status,
                body=body,
                exception=exception,
                headers=headers,
                repeat=repeat,
                response_function=response_function,
            ),
        )

    async def _request_mock(
        self, orig_self: ClientSession, method: str, url: URL | str, *args: tuple[Any], **kwargs: object
    ) -> ClientResponse:
        """Return mocked response object or raise connection error."""
        if orig_self.closed:
            raise RuntimeError('Session is closed')

        url_origin = url
        url = normalize_url(merge_params(url, kwargs.get('params')))
        url_str = str(url)
        for prefix in self._passthrough:
            if url_str.startswith(prefix):
                return await self.patcher.temp_original(orig_self, method, url_origin, *args, **kwargs)

        key = (method, url)
        self.requests.setdefault(key, [])
        request_call = self._build_request_call(method, *args, **kwargs)
        self.requests[key].append(request_call)

        response = await self.match(method, url, **kwargs)

        if response is None:
            raise ClientConnectionError(f'Connection refused: {method} {url}')
        self._responses.append(response)

        # Automatically call response.raise_for_status() on a request if the
        # request was initialized with raise_for_status=True. Also call
        # response.raise_for_status() if the client session was initialized
        # with raise_for_status=True, unless the request was called with
        # raise_for_status=False.
        raise_for_status = kwargs.get('raise_for_status')
        if raise_for_status is None:
            raise_for_status = getattr(orig_self, '_raise_for_status', False)
        if raise_for_status:
            response.raise_for_status()

        return response

    async def match(
        self, method: str, url: URL, allow_redirects: bool = True, **kwargs: Any
    ) -> Optional[ClientResponse]:
        history = []
        while True:
            for mocked_request in self._matches:
                if mocked_request.match(method, url):
                    response_or_exc = await mocked_request.build_response(url, **kwargs)
                    break
            else:
                return None

            if mocked_request.repeat is False:
                self._matches.remove(mocked_request)

            if self.is_exception(response_or_exc):
                raise response_or_exc
            # If response_or_exc was an exception, it would have been raised.
            # At this point we can be sure it's a ClientResponse
            response: ClientResponse
            response = response_or_exc  # type:ignore[assignment]
            is_redirect = response.status in (301, 302, 303, 307, 308)
            if is_redirect and allow_redirects:
                if hdrs.LOCATION not in response.headers:
                    break
                history.append(response)
                redirect_url = URL(response.headers[hdrs.LOCATION])
                if redirect_url.is_absolute():
                    url = redirect_url
                else:
                    url = url.join(redirect_url)
                method = 'get'
                continue
            else:
                break

        response._history = tuple(history)
        return response

    def assert_not_called(self):
        """Assert that the mock was never called."""
        if len(self.requests) != 0:
            msg = "Expected '%s' to not have been called. Called %s times." % (
                self.__class__.__name__,
                len(self._responses),
            )
            raise AssertionError(msg)

    def assert_called(self):
        """Assert that the mock was called at least once."""
        if len(self.requests) == 0:
            msg = "Expected '%s' to have been called." % (self.__class__.__name__,)
            raise AssertionError(msg)

    def assert_called_once(self):
        """Assert that the mock was called only once."""
        call_count = len(self.requests)
        if call_count == 1:
            call_count = len(list(self.requests.values())[0])
        if not call_count == 1:
            msg = "Expected '%s' to have been called once. Called %s times." % (self.__class__.__name__, call_count)

            raise AssertionError(msg)

    def assert_called_with(self, url: Union[URL, str, Pattern], method: str = hdrs.METH_GET, *args: Any, **kwargs: Any):
        """
        Assert that the last call was made with the specified arguments.

        Raises an AssertionError if the args and keyword args passed in are
        different to the last call to the mock.
        """
        url = normalize_url(merge_params(url, kwargs.get('params')))
        method = method.upper()
        key = (method, url)
        try:
            expected = self.requests[key][-1]
        except KeyError:
            expected_string = self._format_call_signature(url, method=method, *args, **kwargs)
            raise AssertionError('%s call not found' % expected_string)
        actual = self._build_request_call(method, *args, **kwargs)
        if not expected == actual:
            expected_string = self._format_call_signature(
                expected,
            )
            actual_string = self._format_call_signature(actual)
            raise AssertionError('%s != %s' % (expected_string, actual_string))

    def assert_any_call(self, url: Union[URL, str, Pattern], method: str = hdrs.METH_GET, *args: Any, **kwargs: Any):
        """
        Assert the mock has been called with the specified arguments.
        The assert passes if the mock has *ever* been called, unlike
        `assert_called_with` and `assert_called_once_with` that only pass if
        the call is the most recent one.
        """
        url = normalize_url(merge_params(url, kwargs.get('params')))
        method = method.upper()
        key = (method, url)

        try:
            self.requests[key]
        except KeyError:
            expected_string = self._format_call_signature(url, method=method, *args, **kwargs)
            raise AssertionError('%s call not found' % expected_string)

    def assert_called_once_with(self, *args: Any, **kwargs: Any):
        """
        Assert that the mock was called once with the specified arguments.
        Raises an AssertionError if the args and keyword args passed in are
        different to the only call to the mock.
        """
        self.assert_called_once()
        self.assert_called_with(*args, **kwargs)

    def _format_call_signature(self, *args, **kwargs) -> str:
        message = '%s(%%s)' % self.__class__.__name__ or 'mock'
        formatted_args = ''
        args_string = ', '.join([repr(arg) for arg in args])
        kwargs_string = ', '.join(['%s=%r' % (key, value) for key, value in kwargs.items()])
        if args_string:
            formatted_args = args_string
        if kwargs_string:
            if formatted_args:
                formatted_args += ', '
            formatted_args += kwargs_string

        return message % formatted_args

    @staticmethod
    def is_exception(resp_or_exc: Union[ClientResponse, Exception]) -> bool:
        if inspect.isclass(resp_or_exc):
            parent_classes = set(inspect.getmro(resp_or_exc))
            if {Exception, BaseException} & parent_classes:
                return True
        elif isinstance(resp_or_exc, (Exception, BaseException)):
            return True
        return False

    def _build_request_call(self, method: str = hdrs.METH_GET, *args: Any, allow_redirects: bool = True, **kwargs: Any):
        kwargs.setdefault('allow_redirects', allow_redirects)
        if method == 'POST':
            kwargs.setdefault('data', None)

        try:
            kwargs_copy = copy.deepcopy(kwargs)
        except (TypeError, ValueError):
            # Handle the fact that some values cannot be deep copied
            kwargs_copy = kwargs
        return RequestCall(args, kwargs_copy)


def merge_params(url: URL | str, params: dict[str, Any] | None = None) -> URL:
    url = URL(url)
    if params:
        query_params = MultiDict(url.query)
        query_params.extend(url.with_query(params).query)
        return url.with_query(query_params)
    return url


def normalize_url(url: Union[URL, str]) -> URL:
    url = URL(url)
    return url.with_query(urlencode(sorted(parse_qsl(url.query_string))))


def get_response_stub(
    request: ProxyRequest, code: int = http.HTTPStatus.OK, headers=None, buffer=None
) -> ClientResponse:
    buffer = utf8(buffer) if buffer else None
    kwargs = {}  # type: dict[str, Any]

    loop = Mock()
    loop.get_debug = Mock()
    loop.get_debug.return_value = True
    kwargs['request_info'] = RequestInfo(
        url=request.url,
        method=request.method,
        headers=CIMultiDictProxy(CIMultiDict({})),
    )
    kwargs['writer'] = None
    kwargs['continue100'] = None
    kwargs['timer'] = TimerNoop()
    kwargs['traces'] = []
    kwargs['loop'] = loop
    kwargs['session'] = None

    if headers is None:
        headers = {}
    _headers = CIMultiDict(**headers)
    raw_headers = tuple([(k.encode('utf8'), v.encode('utf8')) for k, v in _headers.items()])

    resp = ClientResponse(request.method, request.url, **kwargs)
    resp._headers = _headers
    resp._raw_headers = raw_headers
    resp.status = code
    resp.reason = http.RESPONSES[code][0]
    resp.content = StreamReader(ResponseHandler(loop=loop), limit=2**16, loop=loop)
    resp.content.feed_data(buffer)
    resp.content.feed_eof()

    return resp
