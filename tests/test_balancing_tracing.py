from asyncio import Future

from tornado.httpclient import HTTPRequest, HTTPResponse, HTTPError
from tornado.testing import gen_test

from http_client import RequestBuilder
from tests.test_balancing_base import BalancingClientMixin, WorkingServerTestCase


class BalancingTracingTest(BalancingClientMixin, WorkingServerTestCase):

    def setUp(self):
        super().setUp()
        self.register_ports_for_upstream("8081", "8082", "8083")

    def tearDown(self):
        super().tearDown()

    def create_request_balancer(self, ok_server):
        test_request = RequestBuilder("test", "test-app", "/test", 'GET').build()
        return self.request_balancer_builder.build(test_request, None, self.create_execute_request_callback(ok_server),
                                                   None, False, False, False, False)

    def create_execute_request_callback(self, ok_server):
        def execute_request(test_request: HTTPRequest):
            future = Future()
            if test_request.host == ok_server.address:
                future.set_result(HTTPResponse(test_request, 200, request_time=1))
            else:
                error_message = f'Failed to connect to {test_request.host}'
                future.set_result(HTTPResponse(test_request, 599, request_time=0.1,
                                               error=HTTPError(599, error_message)))
            return future

        return execute_request

    # не работает из-за фейковых респонсов выше ^

    # @gen_test
    # async def test_tracing_without_retries(self):
    #     request_balancer = self.create_request_balancer(self.servers[0])
    #     await request_balancer.execute()
    #
    #     expected_trace = "127.0.0.1:8081~200~None"
    #     actual_trace = request_balancer.get_trace()
    #     self.assertEqual(expected_trace, actual_trace)

    # @gen_test
    # async def test_tracing_with_retries(self):
    #     request_balancer = self.create_request_balancer(self.servers[2])
    #     await request_balancer.execute()
    #
    #     expected_trace = "127.0.0.1:8081~599~HTTP 599: Failed to connect to 127.0.0.1:8081 -> " \
    #                      "127.0.0.1:8082~599~HTTP 599: Failed to connect to 127.0.0.1:8082 -> " \
    #                      "127.0.0.1:8083~200~None"
    #     actual_trace = request_balancer.get_trace()
    #     self.assertEqual(expected_trace, actual_trace)
