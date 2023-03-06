from tornado.testing import AsyncHTTPTestCase, gen_test
from tornado.web import RequestHandler, Application
from tornado import gen

from tests.test_balancing_base import BalancingClientMixin


class CoroutineHandler(RequestHandler):
    @gen.coroutine
    def get(self):
        post_result = yield self.application.balancing_client.post_url('test', '/coroutine')
        delete_result = yield self.application.balancing_client.delete_url('test', '/coroutine', parse_on_error=True)
        self.write(f'{post_result.data} {delete_result.data}')

    @gen.coroutine
    def post(self):
        self.set_header('content-type', 'text/plain')
        self.write(b'success')

    @gen.coroutine
    def delete(self):
        self.set_header('content-type', 'text/plain')
        self.set_status(500)
        self.write(b'success')


class BalancingInApplicationTest(BalancingClientMixin, AsyncHTTPTestCase):

    def setUp(self):
        super().setUp()

        self.application.balancing_client = self.balancing_client
        self.register_ports_for_upstream(self.get_http_port())

    def get_app(self):
        self.application = Application([
            (r'/coroutine', CoroutineHandler),
        ])

        return self.application

    @gen_test
    async def test_coroutines(self):
        response = await self.http_client.fetch(f'http://127.0.0.1:{self.get_http_port()}/coroutine')
        self.assertEqual(200, response.code)
        self.assertEqual(b'success success', response.body)
