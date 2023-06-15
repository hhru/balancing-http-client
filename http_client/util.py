import base64
import mimetypes
from urllib.parse import urlencode
from uuid import uuid4
from io import BytesIO

from lxml import etree
from http_client.options import options
from tornado.escape import to_unicode, utf8
from tornado.httpclient import HTTPResponse
from tornado.httputil import HTTPHeaders


def any_to_unicode(s):
    if isinstance(s, bytes):
        return to_unicode(s)

    return str(s)


def any_to_bytes(s):
    if isinstance(s, str):
        return utf8(s)
    elif isinstance(s, bytes):
        return s

    return utf8(str(s))


def make_qs(query_args):
    return urlencode([(k, v) for k, v in query_args.items() if v is not None], doseq=True)


def make_body(data):
    return make_qs(data) if isinstance(data, dict) else any_to_bytes(data)


def make_url(base, **query_args):
    """
    Builds URL from base part and query arguments passed as kwargs.
    Returns unicode string
    """
    qs = make_qs(query_args)

    if qs:
        return to_unicode(base) + ('&' if '?' in base else '?') + qs
    else:
        return to_unicode(base)


def choose_boundary():
    """
    Our embarassingly-simple replacement for mimetools.choose_boundary.
    See https://github.com/kennethreitz/requests/blob/master/requests/packages/urllib3/filepost.py
    """
    return uuid4().hex


BOUNDARY_ = choose_boundary()
BOUNDARY = utf8(BOUNDARY_)


def make_mfd(fields, files):
    """
    Constructs request body in multipart/form-data format

    fields :: { field_name : field_value }
    files :: { field_name: [{ "filename" : fn, "body" : bytes }]}
    """
    def addslashes(text):
        for s in (b'\\', b'"'):
            if s in text:
                text = text.replace(s, b'\\' + s)
        return text

    def create_field(name, data):
        name = addslashes(any_to_bytes(name))

        return [
            b'--', BOUNDARY,
            b'\r\nContent-Disposition: form-data; name="', name,
            b'"\r\n\r\n', any_to_bytes(data), b'\r\n'
        ]

    def create_file_field(name, filename, data, content_type):
        if content_type == 'application/unknown':
            content_type = mimetypes.guess_type(filename)[0] or 'application/octet-stream'
        else:
            content_type = content_type.replace('\n', ' ').replace('\r', ' ')

        name = addslashes(any_to_bytes(name))
        filename = addslashes(any_to_bytes(filename))

        return [
            b'--', BOUNDARY,
            b'\r\nContent-Disposition: form-data; name="', name, b'"; filename="', filename,
            b'"\r\nContent-Type: ', any_to_bytes(content_type),
            b'\r\n\r\n', any_to_bytes(data), b'\r\n'
        ]

    body = []

    for name, data in fields.items():
        if data is None:
            continue

        if isinstance(data, list):
            for value in data:
                if value is not None:
                    body.extend(create_field(name, value))
        else:
            body.extend(create_field(name, data))

    for name, files in files.items():
        for file in files:
            body.extend(create_file_field(
                name, file['filename'], file['body'], file.get('content_type', 'application/unknown')
            ))

    body.extend([b'--', BOUNDARY, b'--\r\n'])
    content_type = 'multipart/form-data; boundary=' + BOUNDARY_

    return b''.join(body), content_type


def response_from_debug(request, response):
    debug_response = etree.XML(response.body)
    original_response = debug_response.find('original-response')

    if original_response is not None:
        response_info = xml_to_dict(original_response)
        original_response.getparent().remove(original_response)

        original_buffer = base64.b64decode(response_info.get('buffer', ''))

        headers = dict(response.headers)
        response_info_headers = response_info.get('headers', {})
        if response_info_headers:
            headers.update(response_info_headers)

        fake_response = HTTPResponse(
            request,
            int(response_info.get('code', 599)),
            headers=HTTPHeaders(headers),
            buffer=BytesIO(original_buffer),
            effective_url=response.effective_url,
            request_time=response.request_time,
            time_info=response.time_info
        )

        return debug_response, fake_response

    return None


def xml_to_dict(xml):
    if len(xml) == 0:
        return xml.text if xml.text is not None else ''

    return {e.tag: xml_to_dict(e) for e in xml}


def restore_original_datacenter_name(datacenter):
    for dc in options.datacenters:
        if dc.lower() == datacenter:
            return dc

    return datacenter
