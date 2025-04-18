import asyncio

import pytest
import yarl
from aiohttp.client_exceptions import ServerTimeoutError
from aiohttp.client_reqrep import ClientResponse

from http_client.request_response import BalancedHttpRequest, RequestResult
from pytest_httpserver import HTTPServer
from tests.test_balancing_base import BalancingClientMixin, TestBase


class TestBalancingTracing(TestBase, BalancingClientMixin):
    @pytest.fixture(scope="function", autouse=True)
    def setup_method(self, working_server: HTTPServer, setup_http_client_factory):
        self.register_ports_for_upstream('8081', '8082', '8083')

    def create_request_balancer(self, ok_server):
        test_request = BalancedHttpRequest('test', 'test-app', '/test', 'GET')
        return self.request_balancer_builder.build(
            test_request, None, self.create_execute_request_callback(ok_server), None, False
        )

    @staticmethod
    def create_execute_request_callback(ok_server):
        async def execute_request(test_request: BalancedHttpRequest) -> RequestResult:
            if test_request.host == ok_server.address:
                url = yarl.URL(test_request.url)
                client_response = ClientResponse(
                    test_request.method,
                    url,
                    writer=None,
                    continue100=None,
                    timer=None,
                    request_info=None,
                    traces=None,
                    loop=asyncio.get_event_loop(),
                    session=None,
                )
                client_response.status = 200
                result = RequestResult(test_request, client_response.status, response=client_response, elapsed_time=1)
                result.parse_response = False
            else:
                error_message = f'Failed to connect to {test_request.host}'
                result = RequestResult(test_request, 599, elapsed_time=0.1, exc=ServerTimeoutError(error_message))

            return result

        return execute_request

    async def test_tracing_without_retries(self):
        request_balancer = self.create_request_balancer(self.servers[0])
        await request_balancer.execute()

        expected_trace = "127.0.0.1:8081~200~None"
        actual_trace = request_balancer.get_trace()
        assert actual_trace == expected_trace

    async def test_tracing_with_retries(self):
        request_balancer = self.create_request_balancer(self.servers[2])
        await request_balancer.execute()

        expected_trace = (
            "127.0.0.1:8081~599~Failed to connect to 127.0.0.1:8081 -> "
            "127.0.0.1:8082~599~Failed to connect to 127.0.0.1:8082 -> "
            "127.0.0.1:8083~200~None"
        )
        actual_trace = request_balancer.get_trace()
        assert actual_trace == expected_trace
