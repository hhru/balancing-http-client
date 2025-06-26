import pytest
from pytest_httpserver import HTTPServer

from http_client.request_response import (
    CLIENT_ERROR,
    DEADLINE_TIMEOUT_MS_HEADER,
    INSUFFICIENT_TIMEOUT,
    OUTER_TIMEOUT_MS_HEADER,
    SERVER_TIMEOUT,
)
from tests.test_balancing_base import BalancingClientMixin, TestBase


class TestBalancingInApplication(TestBase, BalancingClientMixin):
    @pytest.fixture(autouse=True)
    def setup_method(self, working_server: HTTPServer):
        working_server.expect_request('/timeout', method='GET').respond_with_data(
            '', status=200, headers={'content-type': 'text/plain'}
        )

        self.port = working_server.port
        self.register_ports_for_upstream(working_server.port)

    async def test_parse_content(self):
        small_timeout_ms = 2
        small_timeout_s = small_timeout_ms / 1000 / 10

        post_result = await self.balancing_client.get_url('test', '/timeout', request_timeout=small_timeout_s)
        assert post_result.status_code == SERVER_TIMEOUT

        post_result = await self.balancing_client.get_url('test', '/timeout', connect_timeout=small_timeout_s * 0.5)
        assert post_result.status_code == CLIENT_ERROR

        post_result = await self.balancing_client.get_url(
            'test',
            '/timeout',
            request_timeout=small_timeout_s,
            headers={
                OUTER_TIMEOUT_MS_HEADER: small_timeout_ms,
                DEADLINE_TIMEOUT_MS_HEADER: small_timeout_ms + 1,
            },
        )
        assert post_result.status_code == SERVER_TIMEOUT

        post_result = await self.balancing_client.get_url(
            'test',
            '/timeout',
            request_timeout=small_timeout_s,
            headers={
                OUTER_TIMEOUT_MS_HEADER: small_timeout_ms,
                DEADLINE_TIMEOUT_MS_HEADER: small_timeout_ms - 1,
            },
        )
        assert post_result.status_code == INSUFFICIENT_TIMEOUT
