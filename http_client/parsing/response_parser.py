from __future__ import annotations

import re
from enum import Enum
from typing import TYPE_CHECKING, Any, TypeVar, Union

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from multidict import CIMultiDictProxy

json_pattern = re.compile(r'.*json.?')
T = TypeVar('T')


class StatusCodeFamily(Enum):
    CLIENT_ERROR = '4xx'
    SERVER_ERROR = '5xx'

    def match(self, status_code: int) -> bool:
        if self == StatusCodeFamily.CLIENT_ERROR:
            return 400 <= status_code < 500
        elif self == StatusCodeFamily.SERVER_ERROR:
            return status_code >= 500

        return False


Status = TypeVar('Status', bound=(Union[int, tuple[int, ...], StatusCodeFamily]))


def __ensure_json_content_type(headers: CIMultiDictProxy[str]) -> None:
    content_type = headers.get('Content-Type', '')
    if not json_pattern.search(content_type):
        raise ValueError(f'Can only parse json responses, not {content_type}')


def __parse_to_dto(dto_class: type[T], response_body: bytes | None) -> T:
    if hasattr(dto_class, 'load_from_bytes'):
        return dto_class.load_from_bytes(response_body)  # type: ignore[no-any-return, attr-defined]
    elif hasattr(dto_class, 'model_validate_json'):
        return dto_class.model_validate_json(response_body)  # type: ignore[no-any-return, attr-defined]
    else:
        raise ValueError(f'Unexpected Dto class {dto_class}')


def any_to(dto_class: type[T]) -> Callable[[int, bytes | None, CIMultiDictProxy[str]], T]:
    def parsing_function(status_code: int, response_body: bytes | None, headers: CIMultiDictProxy[str]) -> T:
        __ensure_json_content_type(headers)
        return __parse_to_dto(dto_class, response_body)

    return parsing_function


def dict_config(
    config: Mapping[Status, type],
) -> Callable[[int, bytes | None, CIMultiDictProxy[str]], Any]:
    def parsing_function(status_code: int, response_body: bytes | None, headers: CIMultiDictProxy[str]) -> Any:
        __ensure_json_content_type(headers)

        for key, dto_class in config.items():
            if isinstance(key, int) and key == status_code:
                return __parse_to_dto(dto_class, response_body)

        for key, dto_class in config.items():
            if isinstance(key, tuple):
                for status in key:
                    if status == status_code:
                        return __parse_to_dto(dto_class, response_body)

        for key, dto_class in config.items():
            if isinstance(key, StatusCodeFamily) and key.match(status_code):
                return __parse_to_dto(dto_class, response_body)

        raise ValueError(f'Not found dto class for status: {status_code}, in {config}')

    return parsing_function
