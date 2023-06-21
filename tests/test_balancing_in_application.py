from tests.test_balancing_base import BalancingClientMixin, TestBase
from pytest_httpserver import HTTPServer
import pytest


class TestBalancingInApplication(TestBase, BalancingClientMixin):
    @pytest.fixture(scope="function", autouse=True)
    def setup_method(self, working_server: HTTPServer):
        super().setup_method(working_server)

        working_server.expect_request('/content', method='POST').respond_with_data(
            'post_success', status=200, headers={'content-type': 'text/plain'})
        working_server.expect_request('/content', method='DELETE').respond_with_data(
            'delete_success', status=500, headers={'content-type': 'text/plain'})

        self.port = working_server.port
        self.register_ports_for_upstream(working_server.port)

    async def test_parse_content(self):
        post_result = await self.balancing_client.post_url('test', '/content')
        assert post_result.status_code == 200
        assert post_result.data == 'post_success'

        delete_result = await self.balancing_client.delete_url('test', '/content', parse_on_error=True)
        assert delete_result.status_code == 500
        assert delete_result.data == 'delete_success'
