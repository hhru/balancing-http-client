import orjson

try:
    import ujson as json
except ImportError:
    import json

import base64
import logging
import re
from dataclasses import dataclass
from functools import partial
from http.cookies import SimpleCookie
from typing import Generic, Optional, TypeVar

import aiohttp
from aiohttp.client_reqrep import ClientResponse
from aiohttp.typedefs import LooseHeaders
from lxml import etree
from multidict import CIMultiDict

from http_client.options import options
from http_client.util import make_body, make_form_data, make_mfd, make_url, to_unicode, xml_to_dict

USER_AGENT_HEADER = 'User-Agent'
DEADLINE_TIMEOUT_MS_HEADER = 'X-Deadline-Timeout-Ms'
OUTER_TIMEOUT_MS_HEADER = 'X-Outer-Timeout-Ms'
INSUFFICIENT_TIMEOUT = 477
SERVER_TIMEOUT = 577
CLIENT_ERROR = 599

http_client_logger = logging.getLogger('http_client')


@dataclass(frozen=True)
class ResponseData:
    responseCode: Optional[int]
    msg: str


class DataParseError:
    __slots__ = ('attrs',)

    def __init__(self, **attrs):
        self.attrs = attrs


class NoAvailableServerException(Exception):
    pass


class FailFastError(Exception):
    def __init__(self, failed_result: 'RequestResult'):
        self.failed_result = failed_result


class RequestBuilder:
    __slots__ = (
        'body',
        'connect_timeout',
        'follow_redirects',
        'headers',
        'host',
        'idempotent',
        'max_timeout_tries',
        'method',
        'name',
        'path',
        'proxy',
        'request_time_left',
        'request_time_left',
        'request_timeout',
        'session_required',
        'source_app',
        'speculative_timeout_pct',
        'start_time',
        'timeout',
        'upstream_datacenter',
        'upstream_hostname',
        'upstream_name',
        'url',
    )

    def __init__(
        self,
        host: str,
        source_app: str,
        path: str,
        name: str,
        method='GET',
        data=None,
        headers: Optional[LooseHeaders] = None,
        files=None,
        content_type=None,
        connect_timeout=None,
        request_timeout=None,
        max_timeout_tries=None,
        speculative_timeout_pct=None,
        follow_redirects=True,
        idempotent=True,
        use_form_data=False,
    ):
        self.source_app = source_app
        self.host = host.rstrip('/')
        self.path: str = path if path.startswith('/') else '/' + path
        self.name = name
        self.method = method
        self.connect_timeout = connect_timeout
        self.request_timeout = request_timeout
        self.timeout = aiohttp.ClientTimeout(total=request_timeout, connect=connect_timeout)
        self.request_time_left: Optional[float] = None
        self.max_timeout_tries = max_timeout_tries
        self.follow_redirects = follow_redirects
        self.idempotent = idempotent
        self.speculative_timeout_pct = speculative_timeout_pct
        self.body = None
        self.start_time = None
        self.headers = CIMultiDict()
        if headers is not None:
            for key, value in headers.items():
                self.headers.add(key, value if value is not None else '')

        if source_app and not self.headers.get(USER_AGENT_HEADER):
            self.headers[USER_AGENT_HEADER] = source_app

        if self.method == 'POST':
            if use_form_data:
                self.body = make_form_data(data, files)
            else:
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
            self.path = make_url(self.path, **({} if data is None else data))

        if content_type is not None:
            self.headers['Content-Type'] = content_type

        self.upstream_name = self.host
        self.upstream_datacenter = None
        self.upstream_hostname = None

        self.proxy = None
        if options.http_proxy_host is not None:
            self.proxy = f'{options.http_proxy_host}{options.http_proxy_port}'

        if self.host.startswith('http://') or self.host.startswith('https://'):
            self.url = f'{self.host}{self.path}'
        else:
            self.url = f'http://{self.host}{self.path}'


def _parse_response(response_body, real_url, parser, response_type):
    try:
        return parser(response_body)
    except Exception:
        _preview_len = 100

        if response_body is None:
            body_preview = None
        elif len(response_body) > _preview_len:
            body_preview = response_body[:_preview_len]
        else:
            body_preview = response_body

        if body_preview is not None:
            try:
                body_preview = f'excerpt: {to_unicode(body_preview)}'
            except Exception:
                body_preview = f'could not be converted to unicode, excerpt: {body_preview!s}'
        else:
            body_preview = 'is None'

        http_client_logger.exception(
            'failed to parse %s response from %s, body %s', response_type, real_url, body_preview
        )

        return DataParseError(reason=f'invalid {response_type}')


_xml_parser = etree.XMLParser(strip_cdata=False)
_parse_response_xml = partial(
    _parse_response, parser=lambda body: etree.fromstring(body, parser=_xml_parser), response_type='xml'
)


def loads_json(response_body):
    if options.use_orjson:
        return orjson.loads(response_body)
    return json.loads(response_body)


_parse_response_json = partial(_parse_response, parser=loads_json, response_type='json')

_parse_response_text = partial(_parse_response, parser=to_unicode, response_type='text')

RESPONSE_CONTENT_TYPES = {
    'xml': (re.compile(r'.*xml.?'), _parse_response_xml),
    'json': (re.compile(r'.*json.?'), _parse_response_json),
    'text': (re.compile(r'.*text/plain.?'), _parse_response_text),
}


T = TypeVar('T')


class RequestResult(Generic[T]):
    __slots__ = (
        '_content_type',
        '_data',
        '_data_parse_error',
        '_response',
        '_response_body',
        '_status_code',
        'elapsed_time',
        'exc',
        'name',
        'parse_on_error',
        'parse_response',
        'request',
    )

    _args = ('request', '_response', 'parse_response', 'parse_on_error')

    def __init__(
        self,
        request: RequestBuilder,
        status_code: int,
        response: Optional[ClientResponse] = None,
        response_body: Optional[bytes] = None,
        exc=None,
        elapsed_time=None,
        parse_response=True,
        parse_on_error=False,
    ):
        self.name = request.name
        self.request = request

        self.exc = exc
        self.elapsed_time = elapsed_time

        self.parse_response = parse_response
        self.parse_on_error = parse_on_error

        self._status_code: int = status_code
        self._response: Optional[ClientResponse] = response
        self._response_body: Optional[bytes] = response_body
        self._content_type: Optional[str] = None
        self._data: Optional[T] = None
        self._data_parse_error: Optional[DataParseError] = None

    def __repr__(self):
        args = ', '.join(f'{a}={getattr(self, a)!r}' for a in self._args)
        return f'{self.__class__.__name__}({args})'

    def _parse_data(self):
        if self._data is not None or self._data_parse_error is not None:
            return

        if self.exc is not None or (self.status_code >= 400 and not self.parse_on_error):
            self._data_parse_error = DataParseError(reason=self.error, code=self.status_code)
            return

        if not self.parse_response or self.status_code == 204:
            self._data = None if self._response_body == b'' else self._response_body
            self._content_type = 'raw'
            return

        data_or_error = None
        content_type = self.headers.get('Content-Type', '')
        for name, (regex, parser) in RESPONSE_CONTENT_TYPES.items():
            if regex.search(content_type):
                real_url = self.request.url if self.exc is not None else self._response.real_url
                data_or_error = parser(self._response_body, real_url)
                self._content_type = name
                break

        if isinstance(data_or_error, DataParseError):
            self._data_parse_error = data_or_error
        else:
            self._data = data_or_error

    @property
    def status_code(self) -> int:
        return self._status_code

    @property
    def error(self) -> Optional[str]:
        if self._response is not None:
            if self.status_code < 400:
                return None
            return self._response.reason
        return str(self.exc)

    @property
    def headers(self):
        if self._response is not None:
            return self._response.headers
        return CIMultiDict()

    @property
    def cookies(self):
        if self._response is not None:
            return self._response.cookies
        return SimpleCookie()

    @property
    def data(self) -> Optional[T]:
        self._parse_data()
        return self._data

    @property
    def data_parsing_failed(self) -> bool:
        self._parse_data()
        return self._data_parse_error is not None

    @property
    def failed(self):
        return self.exc is not None or self.status_code >= 400 or self.data_parsing_failed

    @property
    def raw_body(self) -> Optional[bytes]:
        return self._response_body

    def to_dict(self):
        self._parse_data()

        if isinstance(self._data_parse_error, DataParseError):
            return {'error': {k: v for k, v in self._data_parse_error.attrs.items()}}

        return self.data if self._content_type == 'json' else None

    def to_etree_element(self):
        self._parse_data()

        if isinstance(self._data_parse_error, DataParseError):
            return etree.Element('error', **{k: str(v) for k, v in self._data_parse_error.attrs.items()})

        return self.data if self._content_type == 'xml' else None

    def get_body_length(self):
        if self._response_body is not None:
            return len(self._response_body)
        return None

    def response_from_debug(self):
        debug_response = etree.XML(self._response_body)
        original_response = debug_response.find('original-response')

        if original_response is not None:
            response_info = xml_to_dict(original_response)
            original_response.getparent().remove(original_response)

            original_buffer = base64.b64decode(response_info.get('buffer', ''))

            headers = CIMultiDict(self.headers)
            response_info_headers = response_info.get('headers', {})

            if response_info_headers:
                headers.update(response_info_headers)

            response = ClientResponse(
                self._response.method,
                self._response.url,
                writer=self._response._writer,
                continue100=self._response._continue,
                timer=self._response._timer,
                request_info=self._response.request_info,
                traces=self._response._traces,
                loop=self._response._loop,
                session=self._response._session,
            )
            response._headers = headers
            response.status = int(response_info.get('code', 599))

            fake_result = RequestResult(
                self.request,
                response.status,
                response,
                original_buffer,
                elapsed_time=self.elapsed_time,
                parse_response=self.parse_response,
                parse_on_error=self.parse_on_error,
            )

            return debug_response, fake_result

        return None
