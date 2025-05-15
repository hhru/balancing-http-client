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


class TestClientPostUrl(TestBase, BalancingClientMixin):
    @pytest.fixture(scope="function", autouse=True)
    def setup_method(self, working_server: HTTPServer, setup_http_client_factory):
        def handler(request: Request):
            assert len(request.files) == 1
            assert FILE_FIELD in request.files
            assert request.files[FILE_FIELD].filename == FILE_NAME
            assert request.files[FILE_FIELD].content_type == FILE_CONTENT_TYPE
            assert request.files[FILE_FIELD].stream.read() == FILE_CONTENT
            assert request.form.get(FIELD_NAME) == FIELD_VALUE
            return Response("good response")

        working_server.expect_request('/echo', method='POST').respond_with_handler(handler)

        self.port = working_server.port
        self.register_ports_for_upstream(working_server.port)

    @pytest.mark.parametrize("use_form_data", [False, True])
    async def test_make_post_request(self, use_form_data):
        post_result = await self.balancing_client.post_url(
            'test',
            '/echo',
            data={FIELD_NAME: FIELD_VALUE},
            files={FILE_FIELD: [{"filename": FILE_NAME, "body": FILE_CONTENT, "content_type": FILE_CONTENT_TYPE}]},
            use_form_data=use_form_data,
        )
        assert post_result.status_code == 200
