import asyncio

import pytest
import yarl
from aiohttp.client_exceptions import ServerTimeoutError
from aiohttp.client_reqrep import ClientResponse
from pytest_httpserver import HTTPServer

from http_client.request_response import RequestBuilder, RequestResult
from tests.test_balancing_base import BalancingClientMixin, TestBase


class TestUpstreamProfiles(TestBase, BalancingClientMixin):
    @pytest.fixture(scope='function', autouse=True)
    def setup_method(self, working_server: HTTPServer, setup_http_client_factory):
        self.register_ports_for_upstream('8081', '8082', '8083')

    def create_request_balancer(self, profile):
        test_request = RequestBuilder('test', 'test-app', '/test', 'GET')
        return self.request_balancer_builder.build(
            test_request, profile, self.create_execute_request_callback(), None, False, False, False, False
        )

    def create_execute_request_callback(self):
        async def execute_request(test_request: RequestBuilder) -> RequestResult:
            if test_request.host == self.servers[2].address:
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

    async def test_profile_with_one_try(self):
        request_balancer = self.create_request_balancer('one_try')
        await request_balancer.execute()

        expected_trace = '127.0.0.1:8081~599~Failed to connect to 127.0.0.1:8081'
        actual_trace = request_balancer.get_trace()
        assert actual_trace, expected_trace

    async def test_profile_with_two_tries(self):
        request_balancer = self.create_request_balancer('two_tries')
        await request_balancer.execute()

        expected_trace = (
            '127.0.0.1:8081~599~Failed to connect to 127.0.0.1:8081 -> '
            '127.0.0.1:8082~599~Failed to connect to 127.0.0.1:8082'
        )
        actual_trace = request_balancer.get_trace()
        assert actual_trace, expected_trace

    async def test_default_profile_with_three_tries(self):
        request_balancer = self.create_request_balancer(None)
        await request_balancer.execute()

        expected_trace = (
            '127.0.0.1:8081~599~Failed to connect to 127.0.0.1:8081 -> '
            '127.0.0.1:8082~599~Failed to connect to 127.0.0.1:8082 -> '
            '127.0.0.1:8083~200~None'
        )
        actual_trace = request_balancer.get_trace()
        assert actual_trace, expected_trace
