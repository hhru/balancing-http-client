import yarl

try:
    import ujson as json
except ImportError:
    import json
import base64
import logging
import re
from dataclasses import dataclass
from functools import partial
from typing import Optional

import aiohttp
from aiohttp.client_reqrep import ClientResponse
from aiohttp.typedefs import LooseHeaders
from lxml import etree
from multidict import CIMultiDict

from http_client.options import options
from http_client.util import (make_body, make_mfd, make_url, to_unicode,
                              xml_to_dict)

USER_AGENT_HEADER = 'User-Agent'

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
        'host', 'path', 'url', 'name', 'method', 'connect_timeout', 'request_timeout', 'timeout', 'request_time_left',
        'max_timeout_tries', 'follow_redirects', 'idempotent', 'speculative_timeout_pct', 'body', 'headers',
        'upstream_name', 'upstream_datacenter', 'proxy', 'session_required', 'request_time_left'
    )

    def __init__(self, host: str, source_app: str, path: str, name: str,
                 method='GET', data=None, headers: Optional[LooseHeaders] = None, files=None, content_type=None,
                 connect_timeout=None, request_timeout=None, max_timeout_tries=None,
                 speculative_timeout_pct=None, follow_redirects=True, idempotent=True):
        self.host = host.rstrip('/')
        self.path = path if path.startswith('/') else '/' + path
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
        self.headers = headers or CIMultiDict()

        if source_app and not self.headers.get(USER_AGENT_HEADER):
            self.headers[USER_AGENT_HEADER] = source_app

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
            self.path = make_url(self.path, **({} if data is None else data))

        if content_type is not None:
            self.headers['Content-Type'] = content_type

        self.upstream_name = self.host
        self.upstream_datacenter = None

        self.proxy = None
        if options.http_proxy_host is not None:
            self.proxy = f'{options.http_proxy_host}{options.http_proxy_port}'

        if self.host.startswith('http://') or self.host.startswith('https://'):
            self.url = f'{self.host}{self.path}'
        else:
            self.url = f'http://{self.host}{self.path}'


def _parse_response(response_body, effective_url, parser, response_type):
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
                body_preview = f'could not be converted to unicode, excerpt: {str(body_preview)}'
        else:
            body_preview = 'is None'

        http_client_logger.exception(
            'failed to parse %s response from %s, body %s',
            response_type, effective_url, body_preview
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


class RequestResult:
    __slots__ = (
        'name', 'request', 'parse_on_error', 'parse_response', '_content_type', '_data',
        '_data_parse_error', 'exc', 'elapsed_time', '_response'
    )

    _args = ('request', '_response', 'parse_response', 'parse_on_error')

    def __init__(self, request: RequestBuilder, response: Optional[ClientResponse] = None, exc=None, elapsed_time=None):
        self.name = request.name
        self.request = request

        self.exc = exc
        self.elapsed_time = elapsed_time

        self.parse_response = True
        self.parse_on_error = False

        self._response = response
        self._content_type = None
        self._data = None
        self._data_parse_error = None

    def __repr__(self):
        args = ', '.join(f'{a}={repr(getattr(self, a))}' for a in self._args)
        return f'{self.__class__.__name__}({args})'

    def _parse_data(self):
        if self._data is not None or self._data_parse_error is not None:
            return

        if self.exc is not None or self.status_code >= 400 and not self.parse_on_error:
            data_or_error = DataParseError(reason=self.error, code=self.status_code)
        elif not self.parse_response or self.status_code == 204:
            data_or_error = self._response._body
            self._content_type = 'raw'
        else:
            data_or_error = None
            content_type = self.headers.get('Content-Type', '')
            for name, (regex, parser) in RESPONSE_CONTENT_TYPES.items():
                if regex.search(content_type):
                    effective_url = self.request.url if self.exc is not None else self._response.real_url
                    data_or_error = parser(self._response._body, effective_url)
                    self._content_type = name
                    break

        if isinstance(data_or_error, DataParseError):
            self._data_parse_error = data_or_error
        else:
            self._data = data_or_error

    @property
    def status_code(self):
        if self._response is not None:
            return self._response.status
        return 599

    @property
    def error(self) -> Optional[str]:
        if self._response is not None:
            return self._response.reason
        return str(self.exc)

    @property
    def headers(self):
        if self._response is not None:
            return self._response.headers
        return CIMultiDict()

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
        return self.exc is not None or self.status_code >= 400 or self.data_parsing_failed

    @property
    def raw_body(self):
        if self._response is not None:
            return self._response._body
        return None

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

    def get_body_length(self):
        if self._response is not None and self._response._body is not None:
            return len(self._response._body)
        return None

    def response_from_debug(self):
        debug_response = etree.XML(self._response._body)
        original_response = debug_response.find('original-response')

        if original_response is not None:
            response_info = xml_to_dict(original_response)
            original_response.getparent().remove(original_response)

            original_buffer = base64.b64decode(response_info.get('buffer', ''))

            headers = CIMultiDict(self.headers)
            response_info_headers = response_info.get('headers', {})

            if response_info_headers:
                headers.update(response_info_headers)

            response = ClientResponse(self._response.method, self._response.url, writer=self._response._writer,
                                      continue100=self._response._continue, timer=self._response._timer,
                                      request_info=self._response.request_info, traces=self._response._traces,
                                      loop=self._response._loop, session=self._response._session)
            response._headers = headers
            response._body = original_buffer
            response.status = int(response_info.get('code', 599))

            fake_result = RequestResult(
                self.request,
                response,
                elapsed_time=self.elapsed_time,
            )

            return debug_response, fake_result

        return None


class TornadoResponseWrapper:
    """
    only for testing

    Attributes
    ----------
    resp : tornado.httpclient.HTTPResponse
    """
    def __init__(self, resp):
        self.resp = resp

    @property
    def status(self):
        return self.resp.code

    @property
    def reason(self):
        return self.resp.reason

    @property
    def _body(self):
        return self.resp.body

    @property
    def headers(self):
        return self.resp.headers

    @property
    def real_url(self):
        return self.resp.effective_url
