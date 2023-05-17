from tornado.testing import AsyncHTTPTestCase, gen_test
from tornado.web import RequestHandler, Application

from tests.test_balancing_base import BalancingClientMixin


class PageHandler(RequestHandler):
    def get(self):
        self.write('ok')


class TestHttpClientParameters(BalancingClientMixin, AsyncHTTPTestCase):

    def setUp(self):
        super().setUp()

        self.application.balancing_client = self.balancing_client
        self.register_ports_for_upstream(self.get_http_port())

    def get_app(self):
        self.application = Application([
            (r'/', PageHandler),
        ])

        return self.application

    @gen_test
    async def test_nullable_headers(self):
        result = await self.balancing_client.get_url('test', '/', headers={'NoneHeader': None, 'NotNoneHeader': '123'})

        none_header = result.request.headers.get_list('NoneHeader')
        not_none_header = result.request.headers.get_list('Notnoneheader')

        self.assertEqual(200, result.response.code)
        self.assertEqual(b'ok', result.response.body)
        self.assertEqual(0, len(none_header))
        self.assertEqual('123', not_none_header[0])
