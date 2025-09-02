from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any
from unittest.mock import Mock

import pytest
from multidict import CIMultiDict
from pydantic import BaseModel

from http_client.parsing.response_parser import StatusCodeFamily, any_to, dict_config
from http_client.request_response import ParsingError, RequestBuilder, RequestResult

if TYPE_CHECKING:
    from multidict import CIMultiDictProxy


class UserDTO(BaseModel):
    id: int
    name: str
    email: str


class ErrorDTO(BaseModel):
    error: str
    code: int


class NotFoundDTO(BaseModel):
    message: str
    resource: str


class SuccessDTO(BaseModel):
    status: str
    data: dict[str, str]


class CustomDTO:
    def __init__(self, data: dict[str, str]) -> None:
        self.data = data

    @classmethod
    def load_from_bytes(cls, data: bytes) -> CustomDTO:
        return cls(json.loads(data.decode('utf-8')))


class TestResponseParsing:
    @pytest.fixture
    def request_result(self) -> RequestResult[Any]:
        json_response_body: bytes = json.dumps({'id': 1, 'name': 'John Doe', 'email': 'john@example.com'}).encode(
            'utf-8'
        )

        mock_response = Mock()
        mock_response.headers = CIMultiDict({'Content-Type': 'application/json'})

        mock_request = Mock(spec=RequestBuilder)

        return RequestResult(
            request=mock_request,
            status_code=200,
            response=mock_response,
            response_body=json_response_body,
            elapsed_time=0.1,
        )

    def test_parse_with_simple_type(self, request_result: RequestResult[UserDTO]) -> None:
        result: UserDTO = request_result.parse(UserDTO)

        assert isinstance(result, UserDTO)
        assert result.id == 1
        assert result.name == 'John Doe'
        assert result.email == 'john@example.com'

    def test_parse_with_function(self, request_result: RequestResult[UserDTO]) -> None:
        result: UserDTO = request_result.parse_with(any_to(UserDTO))

        assert isinstance(result, UserDTO)
        assert result.id == 1
        assert result.name == 'John Doe'
        assert result.email == 'john@example.com'

    def test_parse_with_custom_dto_class(self, request_result: RequestResult[CustomDTO]) -> None:
        request_result._response_body = json.dumps({'key': 'value'}).encode('utf-8')

        result: CustomDTO = request_result.parse(CustomDTO)

        assert isinstance(result, CustomDTO)
        assert result.data == {'key': 'value'}

    def test_parse_with_dict_mapping_simple(self, request_result: RequestResult[Any]) -> None:
        status_mapping: dict[int, type] = {200: UserDTO, 404: NotFoundDTO, 500: ErrorDTO}

        result: UserDTO | NotFoundDTO | ErrorDTO = request_result.parse_with(dict_config(status_mapping))

        assert isinstance(result, UserDTO)
        assert result.id == 1
        assert result.name == 'John Doe'

    def test_parse_with_dict_mapping_tuple(self, request_result: RequestResult[Any]) -> None:
        status_mapping: dict[int | tuple[int, ...], type] = {(200, 201): UserDTO, 404: NotFoundDTO, 500: ErrorDTO}

        result: UserDTO | NotFoundDTO | ErrorDTO = request_result.parse_with(dict_config(status_mapping))

        assert isinstance(result, UserDTO)
        assert result.id == 1
        assert result.name == 'John Doe'

    def test_parse_with_dict_mapping_enum(self, request_result: RequestResult[Any]) -> None:
        request_result.status_code = 503
        request_result._response_body = json.dumps({'error': 'Service Unavailable', 'code': 503}).encode('utf-8')
        status_mapping: dict[int | StatusCodeFamily, type] = {200: UserDTO, StatusCodeFamily.SERVER_ERROR: ErrorDTO}

        result: UserDTO | ErrorDTO = request_result.parse_with(dict_config(status_mapping))

        assert isinstance(result, ErrorDTO)
        assert result.error == 'Service Unavailable'
        assert result.code == 503

    def test_parse_with_dict_mapping_unexpected(self, request_result: RequestResult[Any]) -> None:
        request_result.status_code = 503
        status_mapping: dict[int, type] = {200: UserDTO, 400: ErrorDTO}

        with pytest.raises(ParsingError):
            request_result.parse_with(dict_config(status_mapping))

    def test_parse_with_user_function(self, request_result: RequestResult[Any]) -> None:
        request_result.status_code = 404
        request_result._response_body = json.dumps({'error': 'Not found', 'code': 404}).encode('utf-8')

        def custom_parse(
            status_code: int, response_body: bytes | None, headers: CIMultiDictProxy[str]
        ) -> UserDTO | ErrorDTO:
            assert response_body is not None
            error = json.loads(response_body)['error']
            return ErrorDTO(error=error, code=status_code)

        result: UserDTO | ErrorDTO = request_result.parse_with(custom_parse)

        assert isinstance(result, ErrorDTO)
        assert result.error == 'Not found'
        assert result.code == 404
