import mimetypes
from typing import Any, Dict, List
from urllib.parse import urlencode
from uuid import uuid4
import random

from aiohttp import FormData

from http_client.options import options


def to_unicode(value: Any) -> str:
    if isinstance(value, (str, type(None))):
        return value
    if not isinstance(value, bytes):
        raise TypeError(f'Expected bytes; got {type(value)}')
    return value.decode('utf-8')


def utf8(value: str) -> bytes:
    if not isinstance(value, str):
        raise TypeError('Expected unicode str; got {type(value)}')
    return value.encode('utf-8')


def any_to_unicode(s):
    if isinstance(s, bytes):
        return to_unicode(s)

    return str(s)


def any_to_bytes(s: Any) -> bytes:
    if isinstance(s, str):
        return utf8(s)
    elif isinstance(s, bytes):
        return s

    return utf8(str(s))


def make_qs(query_args) -> str:
    return urlencode([(k, v) for k, v in query_args.items() if v is not None], doseq=True)


def make_qs_bytes(query_args) -> bytes:
    return make_qs(query_args).encode('ascii')


def make_body(data) -> bytes:
    return make_qs_bytes(data) if isinstance(data, dict) else any_to_bytes(data)


def make_url(base: str, **query_args) -> str:
    """
    Builds URL from base part and query arguments passed as kwargs.
    Returns unicode string
    """
    qs = make_qs(query_args)

    if qs:
        return f"{base}{'&' if '?' in base else '?'}{qs}"
    else:
        return base


def choose_boundary():
    """
    Our embarassingly-simple replacement for mimetools.choose_boundary.
    See https://github.com/kennethreitz/requests/blob/master/requests/packages/urllib3/filepost.py
    """
    return uuid4().hex


BOUNDARY_ = choose_boundary()
BOUNDARY = utf8(BOUNDARY_)


def make_form_data(fields: Dict[str, str], files: Dict[str, List[Dict[str, Any]]]) -> FormData:
    form = FormData(fields=list(fields.items()) if fields else [])
    if files:
        for name, files_ in files.items():
            for file_dict in files_:
                form.add_field(
                    name=name,
                    value=file_dict['body'],
                    filename=file_dict['filename'],
                    content_type=file_dict.get('content_type', 'application/unknown'),
                )
    return form


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
            b'--',
            BOUNDARY,
            b'\r\nContent-Disposition: form-data; name="',
            name,
            b'"\r\n\r\n',
            any_to_bytes(data),
            b'\r\n',
        ]

    def create_file_field(name, filename, data, content_type):
        if content_type == 'application/unknown':
            content_type = mimetypes.guess_type(filename)[0] or 'application/octet-stream'
        else:
            content_type = content_type.replace('\n', ' ').replace('\r', ' ')

        name = addslashes(any_to_bytes(name))
        filename = addslashes(any_to_bytes(filename))

        return [
            b'--',
            BOUNDARY,
            b'\r\nContent-Disposition: form-data; name="',
            name,
            b'"; filename="',
            filename,
            b'"\r\nContent-Type: ',
            any_to_bytes(content_type),
            b'\r\n\r\n',
            any_to_bytes(data),
            b'\r\n',
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
            body.extend(
                create_file_field(name, file['filename'], file['body'], file.get('content_type', 'application/unknown'))
            )

    body.extend([b'--', BOUNDARY, b'--\r\n'])
    content_type = 'multipart/form-data; boundary=' + BOUNDARY_

    return b''.join(body), content_type


def xml_to_dict(xml):
    if len(xml) == 0:
        return xml.text if xml.text is not None else ''

    return {e.tag: xml_to_dict(e) for e in xml}


def restore_original_datacenter_name(datacenter):
    for dc in options.datacenters:
        if dc.lower() == datacenter:
            return dc

    return datacenter


def weighted_sample(elems: list, weights: list, k: int, sum_weight: int) -> list:
    n = len(elems)

    if len(weights) != n:
        raise ValueError('wrong elems/weights sizes')
    if k > n:
        raise ValueError('sample can not be bigger than source')

    result = []
    ids = list(range(n))

    for _ in range(k):
        pick = random.random() * sum_weight
        i = 0
        sub_sum = weights[0]

        while sub_sum < pick:
            i += 1
            sub_sum += weights[i]

        result.append(elems[ids[i]])
        sum_weight -= weights[ids[i]]

        ids[i], ids[n - 1] = ids[n - 1], ids[i]
        weights[i], weights[n - 1] = weights[n - 1], weights[i]
        n -= 1

    return result
