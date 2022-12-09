from tests.test_balancing_base import BalancingClientMixin, WorkingServerTestCase
from tornado.testing import gen_test
from tornado.httpclient import HTTPRequest, HTTPResponse, HTTPError
from asyncio import Future
from http_client import RequestBuilder
from http_client.balancing import BalancingState, UpstreamRequestBalancer, Upstream, Server


class BalancingTracingTest(BalancingClientMixin, WorkingServerTestCase):

    def setUp(self):
        super().setUp()

        self.servers = [
            Server("127.0.0.1:8081", dc="test"),
            Server("127.0.0.1:8082", dc="test"),
            Server("127.0.0.1:8083", dc="test")
        ]
        upstream_config = {"max_tries": 3, "request_timeout_sec": 10}
        upstream = Upstream("test-service", upstream_config, self.servers)

        state = BalancingState(upstream)
        test_request = RequestBuilder("test-service", "test-app", "/test", 'GET').build()
        self.request_balancer = UpstreamRequestBalancer(state, test_request, None, None, False,
                                                        None, False, False, False, None, None)

    def tearDown(self):
        super().tearDown()

    @staticmethod
    def create_execute_request_callback(ok_server):
        def execute_request(test_request: HTTPRequest):
            future = Future()
            if test_request.host == ok_server.address:
                future.set_result(HTTPResponse(test_request, 200, request_time=1))
            else:
                error_message = f'Failed to connect to {test_request.host}'
                future.set_result(HTTPResponse(test_request, 599, request_time=2, error=HTTPError(599, error_message)))
            return future
        return execute_request

    @gen_test
    async def test_tracing_without_retries(self):
        self.request_balancer.execute_request = self.create_execute_request_callback(self.servers[0])

        await self.request_balancer.execute()

        expected_trace = "127.0.0.1:8081~200~None"
        actual_trace = self.request_balancer.get_trace()
        self.assertEqual(expected_trace, actual_trace)

    @gen_test
    async def test_tracing_with_retries(self):
        self.request_balancer.execute_request = self.create_execute_request_callback(self.servers[2])

        await self.request_balancer.execute()

        expected_trace = "127.0.0.1:8081~599~HTTP 599: Failed to connect to 127.0.0.1:8081 -> " \
                         "127.0.0.1:8082~599~HTTP 599: Failed to connect to 127.0.0.1:8082 -> " \
                         "127.0.0.1:8083~200~None"
        actual_trace = self.request_balancer.get_trace()
        self.assertEqual(expected_trace, actual_trace)
