import threading
from functools import partial
from http import HTTPStatus

from tornado.httpclient import HTTPRequest, HTTPError
from tornado.testing import gen_test, bind_unused_port

from tests.test_balancing_base import BalancingClientMixin, WorkingServerTestCase


def low_backlog_server_handler(sock, event):
    sock.listen(1)
    event.wait()


class LowBacklogTest(BalancingClientMixin, WorkingServerTestCase):

    def setUp(self):
        self.low_backlog_server_socket, low_backlog_server_port = bind_unused_port()
        self.stop_event = threading.Event()
        low_backlog_server = threading.Thread(target=low_backlog_server_handler,
                                              args=(self.low_backlog_server_socket, self.stop_event))
        low_backlog_server.daemon = True
        low_backlog_server.start()

        super().setUp()
        self.register_ports_for_upstream(low_backlog_server_port, self.get_http_port())

        self.io_loop.run_sync(partial(self.fill_backlog, low_backlog_server_port))

    async def fill_backlog(self, port):
        request = HTTPRequest(f'http://127.0.0.1:{port}', connect_timeout=0.1, request_timeout=0.1)
        for i in range(3):
            try:
                await self.http_client.fetch(request)
            except HTTPError:
                pass

    def tearDown(self):
        super().tearDown()
        self.stop_event.set()
        self.low_backlog_server_socket.close()

    @gen_test
    async def test_low_backlog_idempotent_retries(self):
        response = await self.balancing_client.get_url('test', '/')
        self.assertEqual(HTTPStatus.OK, response.response.code)

    @gen_test
    async def test_low_backlog_non_idempotent_retries(self):
        response = await self.balancing_client.post_url('test', '/')
        self.assertEqual(HTTPStatus.OK, response.response.code)
