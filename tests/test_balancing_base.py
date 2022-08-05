from tornado.httpclient import AsyncHTTPClient
from tornado.testing import AsyncHTTPTestCase
from tornado.web import RequestHandler, Application

from http_client import HttpClientFactory, Server, Upstream, options


class WorkingHandler(RequestHandler):
    def get(self):
        self.set_status(200)

    def post(self):
        self.set_status(200)


class WorkingServerTestCase(AsyncHTTPTestCase):

    def get_app(self):
        return Application([
            (r"/", WorkingHandler),
        ])


class BalancingClientMixin:

    def setUp(self):
        AsyncHTTPClient.configure('tornado.curl_httpclient.CurlAsyncHTTPClient')
        super().setUp()
        self.http_client_factory = HttpClientFactory('testapp', self.http_client)
        self.balancing_client = self.http_client_factory.get_http_client()
        options.datacenter = 'test'

    def get_upstream_config(self):
        return {'request_timeout_sec': 0.5}

    def register_ports_for_upstream(self, *ports):
        upstream = Upstream('test', self.get_upstream_config(),
                            [Server(f'127.0.0.1:{port}', dc='test') for port in ports])
        self.http_client_factory.upstreams[upstream.name] = upstream
