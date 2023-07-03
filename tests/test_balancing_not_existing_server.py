from http import HTTPStatus

import pytest

from pytest_httpserver import HTTPServer
from tests.test_balancing_base import BalancingClientMixin, TestBase


class TestNotExistingServer(TestBase, BalancingClientMixin):
    @pytest.fixture(scope="function", autouse=True)
    def setup_method(self, working_server: HTTPServer):
        self.not_serving_socket, not_serving_port = self.bind_unused_port()
        self.not_serving_socket.close()
        super().setup_method(working_server)
        self.register_ports_for_upstream(not_serving_port, working_server.port)

    async def test_non_existing_idempotent_retries(self):
        result = await self.balancing_client.get_url('test', '/')
        assert result.status_code == HTTPStatus.OK

    async def test_non_existing_non_idempotent_retries(self):
        result = await self.balancing_client.post_url('test', '/')
        assert result.status_code == HTTPStatus.OK
