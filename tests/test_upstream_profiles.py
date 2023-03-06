from asyncio import Future

from tornado.httpclient import HTTPRequest, HTTPResponse, HTTPError
from tornado.testing import gen_test

from http_client import RequestBuilder
from tests.test_balancing_base import BalancingClientMixin, WorkingServerTestCase


class UpstreamProfilesTest(BalancingClientMixin, WorkingServerTestCase):

    def setUp(self):
        super().setUp()
        self.register_ports_for_upstream("8081", "8082", "8083")

    def tearDown(self):
        super().tearDown()

    def create_request_balancer(self, profile):
        test_request = RequestBuilder("test", "test-app", "/test", 'GET').build()
        return self.request_balancer_builder.build(test_request, profile, self.create_execute_request_callback(),
                                                   None, False, False, False, False)

    def create_execute_request_callback(self):
        def execute_request(test_request: HTTPRequest):
            future = Future()
            if test_request.host == self.servers[2].address:
                future.set_result(HTTPResponse(test_request, 200, request_time=1))
            else:
                error_message = f'Failed to connect to {test_request.host}'
                future.set_result(HTTPResponse(test_request, 599, request_time=0.1,
                                               error=HTTPError(599, error_message)))
            return future

        return execute_request

    @gen_test
    async def test_profile_with_one_try(self):
        request_balancer = self.create_request_balancer("one_try")
        await request_balancer.execute()

        expected_trace = "127.0.0.1:8081~599~HTTP 599: Failed to connect to 127.0.0.1:8081"
        actual_trace = request_balancer.get_trace()
        self.assertEqual(expected_trace, actual_trace)

    @gen_test
    async def test_profile_with_two_tries(self):
        request_balancer = self.create_request_balancer("two_tries")
        await request_balancer.execute()

        expected_trace = "127.0.0.1:8081~599~HTTP 599: Failed to connect to 127.0.0.1:8081 -> " \
                         "127.0.0.1:8082~599~HTTP 599: Failed to connect to 127.0.0.1:8082"
        actual_trace = request_balancer.get_trace()
        self.assertEqual(expected_trace, actual_trace)

    @gen_test
    async def test_default_profile_with_three_tries(self):
        request_balancer = self.create_request_balancer(None)
        await request_balancer.execute()

        expected_trace = "127.0.0.1:8081~599~HTTP 599: Failed to connect to 127.0.0.1:8081 -> " \
                         "127.0.0.1:8082~599~HTTP 599: Failed to connect to 127.0.0.1:8082 -> " \
                         "127.0.0.1:8083~200~None"
        actual_trace = request_balancer.get_trace()
        self.assertEqual(expected_trace, actual_trace)
