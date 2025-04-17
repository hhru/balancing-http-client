import pytest

from pytest_httpserver import HTTPServer
from werkzeug import Request, Response

from tests.test_balancing_base import BalancingClientMixin, TestBase

FILE_FIELD = "file1"
FILE_NAME = "filename"
FILE_CONTENT_TYPE = "file_content_type"
FILE_CONTENT = b"file_content"

FIELD_NAME = "field1"
FIELD_VALUE = "field1_value"

DEFAULT_BODY = {FIELD_NAME: FIELD_VALUE}


class TestClientPostUrl(TestBase, BalancingClientMixin):
    @pytest.fixture(scope="function", autouse=True)
    def setup_method(self, working_server: HTTPServer, setup_http_client_factory):
        def file_handler(request: Request):
            assert len(request.files) == 1
            assert FILE_FIELD in request.files
            assert request.files[FILE_FIELD].filename == FILE_NAME
            assert request.files[FILE_FIELD].content_type == FILE_CONTENT_TYPE
            assert request.files[FILE_FIELD].stream.read() == FILE_CONTENT
            assert request.form.get(FIELD_NAME) == FIELD_VALUE
            return Response("good response")

        def handler(request: Request):
            assert request.form.get(FIELD_NAME) == FIELD_VALUE
            return Response("good response")

        def json_handler(request: Request):
            assert request.json == DEFAULT_BODY
            return Response("good response")

        working_server.expect_request("/echo_with_file", method="POST").respond_with_handler(file_handler)
        working_server.expect_request("/echo_json", method="POST").respond_with_handler(json_handler)
        working_server.expect_request("/echo", method="POST").respond_with_handler(handler)

        self.port = working_server.port
        self.register_ports_for_upstream(working_server.port)

    async def test_make_post_request_with_files(self):
        post_result = await self.balancing_client.post_url(
            "test",
            "/echo_with_file",
            data=DEFAULT_BODY,
            files={
                FILE_FIELD: [
                    {
                        "filename": FILE_NAME,
                        "body": FILE_CONTENT,
                        "content_type": FILE_CONTENT_TYPE,
                    }
                ]
            },
        )
        assert post_result.status_code == 200

    async def test_make_post_request(self):
        post_result = await self.balancing_client.post_url(
            "test",
            "/echo",
            data=DEFAULT_BODY,
        )
        assert post_result.status_code == 200

    async def test_make_post_json_request(self):
        post_result = await self.balancing_client.post_url(
            "test", "/echo_json", data=DEFAULT_BODY, content_type="application/json"
        )
        assert post_result.status_code == 200
