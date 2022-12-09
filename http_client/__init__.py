try:
    import ujson as json
except ImportError:
    import json
import logging
import re
from asyncio import Future
from dataclasses import dataclass
from functools import partial

import pycurl
from lxml import etree
from tornado.curl_httpclient import CurlAsyncHTTPClient
from tornado.escape import to_unicode
from tornado.httpclient import HTTPRequest, HTTPResponse
from tornado.httputil import HTTPHeaders

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


@dataclass(frozen=True)
class ResponseData:
    responseCode: int
    msg: str


class RequestBuilder:
    def __init__(self, host: str, source_app: str, uri: str, name: str,
                 method='GET', data=None, headers=None, files=None, content_type=None,
                 connect_timeout=None, request_timeout=None, max_timeout_tries=None,
                 speculative_timeout_pct=None, follow_redirects=True, idempotent=True):
        self.source_app = source_app
        self.host = host.rstrip('/')
        self.uri = uri if uri.startswith('/') else '/' + uri
        self.name = name
        self.method = method
        self.connect_timeout = connect_timeout
        self.request_timeout = request_timeout
        self.max_timeout_tries = max_timeout_tries
        self.follow_redirects = follow_redirects
        self.idempotent = idempotent
        self.speculative_timeout_pct = speculative_timeout_pct
        self.body = None

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

    def build(self) -> HTTPRequest:
        request = HTTPRequest(
            url=self.host + self.uri,
            body=self.body,
            method=self.method,
            headers=self.headers,
            follow_redirects=self.follow_redirects,
            connect_timeout=self.connect_timeout,
            request_timeout=self.request_timeout,
        )

        request.source_app = self.source_app
        request.host = self.host
        request.uri = self.uri
        request.name = self.name
        request.max_timeout_tries = self.max_timeout_tries
        request.idempotent = self.idempotent
        request.speculative_timeout_pct = self.speculative_timeout_pct

        request.upstream_name = None
        request.upstream_datacenter = None

        if options.http_proxy_host is not None:
            request.proxy_host = options.http_proxy_host
            request.proxy_port = options.http_proxy_port

        return request


class RequestEngine:

    def execute(self) -> 'Future[RequestResult]':
        pass


class RequestEngineBuilder:

    def build(self, request: HTTPRequest, execute_request, modify_http_request_hook, debug_mode, callback,
              parse_response, parse_on_error, fail_fast) -> RequestEngine:
        pass


class HttpClientFactory:
    def __init__(self, source_app, tornado_http_client, request_engine_builder: RequestEngineBuilder):
        self.tornado_http_client = tornado_http_client
        self.source_app = source_app
        self.request_engine_builder = request_engine_builder

    def get_http_client(self, modify_http_request_hook=None, debug_mode=False):
        return HttpClient(
            self.tornado_http_client,
            self.source_app,
            self.request_engine_builder,
            modify_http_request_hook=modify_http_request_hook,
            debug_mode=debug_mode
        )


class HttpClient:
    @staticmethod
    def _prepare_curl_callback(curl, next_callback):
        curl.setopt(pycurl.NOSIGNAL, 1)

        if callable(next_callback):
            next_callback(curl)

    def __init__(self, http_client_impl, source_app, request_engine_builder: RequestEngineBuilder, *,
                 modify_http_request_hook=None, debug_mode=False):
        self.http_client_impl = http_client_impl
        self.source_app = source_app
        self.debug_mode = debug_mode
        self.modify_http_request_hook = modify_http_request_hook
        self.request_engine_builder = request_engine_builder

    def get_url(self, host, uri, *, name=None, data=None, headers=None, follow_redirects=True,
                connect_timeout=None, request_timeout=None, max_timeout_tries=None,
                callback=None, parse_response=True, parse_on_error=False, fail_fast=False,
                speculative_timeout_pct=None):

        request = RequestBuilder(
            host, self.source_app, uri, name, 'GET', data, headers, None, None,
            connect_timeout, request_timeout, max_timeout_tries, speculative_timeout_pct, follow_redirects
        ).build()

        request_engine = self.request_engine_builder.build(request, self.execute_request, self.modify_http_request_hook,
                                                           self.debug_mode, callback, parse_response, parse_on_error,
                                                           fail_fast)
        return request_engine.execute()

    def head_url(self, host, uri, *, name=None, data=None, headers=None, follow_redirects=True,
                 connect_timeout=None, request_timeout=None, max_timeout_tries=None,
                 callback=None, fail_fast=False, speculative_timeout_pct=None):

        request = RequestBuilder(
            host, self.source_app, uri, name, 'HEAD', data, headers, None, None,
            connect_timeout, request_timeout, max_timeout_tries, speculative_timeout_pct, follow_redirects
        ).build()

        request_engine = self.request_engine_builder.build(request, self.execute_request, self.modify_http_request_hook,
                                                           self.debug_mode, callback, False, False, fail_fast)
        return request_engine.execute()

    def post_url(self, host, uri, *,
                 name=None, data='', headers=None, files=None, content_type=None, follow_redirects=True,
                 connect_timeout=None, request_timeout=None, max_timeout_tries=None, idempotent=False,
                 callback=None, parse_response=True, parse_on_error=False, fail_fast=False,
                 speculative_timeout_pct=None):

        request = RequestBuilder(
            host, self.source_app, uri, name, 'POST', data, headers, files, content_type,
            connect_timeout, request_timeout, max_timeout_tries, speculative_timeout_pct, follow_redirects, idempotent
        ).build()

        request_engine = self.request_engine_builder.build(request, self.execute_request, self.modify_http_request_hook,
                                                           self.debug_mode, callback, parse_response, parse_on_error,
                                                           fail_fast)
        return request_engine.execute()

    def put_url(self, host, uri, *, name=None, data='', headers=None, content_type=None, follow_redirects=True,
                connect_timeout=None, request_timeout=None, max_timeout_tries=None, idempotent=True,
                callback=None, parse_response=True, parse_on_error=False, fail_fast=False,
                speculative_timeout_pct=None):

        request = RequestBuilder(
            host, self.source_app, uri, name, 'PUT', data, headers, None, content_type,
            connect_timeout, request_timeout, max_timeout_tries, speculative_timeout_pct, follow_redirects, idempotent
        ).build()

        request_engine = self.request_engine_builder.build(request, self.execute_request, self.modify_http_request_hook,
                                                           self.debug_mode, callback, parse_response, parse_on_error,
                                                           fail_fast)
        return request_engine.execute()

    def delete_url(self, host, uri, *, name=None, data=None, headers=None, content_type=None,
                   connect_timeout=None, request_timeout=None, max_timeout_tries=None,
                   callback=None, parse_response=True, parse_on_error=False, fail_fast=False,
                   speculative_timeout_pct=None):

        request = RequestBuilder(
            host, self.source_app, uri, name, 'DELETE', data, headers, None, content_type,
            connect_timeout, request_timeout, max_timeout_tries, speculative_timeout_pct
        ).build()

        request_engine = self.request_engine_builder.build(request, self.execute_request, self.modify_http_request_hook,
                                                           self.debug_mode, callback, parse_response, parse_on_error,
                                                           fail_fast)
        return request_engine.execute()

    def execute_request(self, request: HTTPRequest):
        if isinstance(self.http_client_impl, CurlAsyncHTTPClient):
            request.prepare_curl_callback = partial(
                self._prepare_curl_callback, next_callback=request.prepare_curl_callback
            )

        return self.http_client_impl.fetch(request)


class DataParseError:
    __slots__ = ('attrs',)

    def __init__(self, **attrs):
        self.attrs = attrs


class RequestResult:
    __slots__ = (
        'name', 'request', 'response', 'parse_on_error', 'parse_response', '_content_type', '_data', '_data_parse_error'
    )

    _args = ('request', 'response', 'parse_response', 'parse_on_error')

    def __init__(self, request: HTTPRequest, response: HTTPResponse,
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
