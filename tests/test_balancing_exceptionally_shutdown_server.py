import asyncio
import threading
from http import HTTPStatus

from tornado.testing import gen_test, bind_unused_port

from tests.test_balancing_base import BalancingClientMixin, WorkingServerTestCase


def exceptionally_server(sock):
    sock.listen(10)
    while True:
        try:
            client_sock, client_addr = sock.accept()
            client_sock.recv(4)
            raise Exception()
        except OSError:
            pass


class ExceptionalShutdownTest(BalancingClientMixin, WorkingServerTestCase):

    def setUp(self):
        self.exceptionally_shutdown_server_socket, exceptionally_shutdown_server_port = bind_unused_port()
        exceptionally_server_thread = threading.Thread(target=exceptionally_server,
                                                       args=(self.exceptionally_shutdown_server_socket,))
        exceptionally_server_thread.daemon = True
        exceptionally_server_thread.start()
        super().setUp()
        self.register_ports_for_upstream(exceptionally_shutdown_server_port, self.get_http_port())

    def tearDown(self):
        super().tearDown()
        self.exceptionally_shutdown_server_socket.close()

    # @gen_test
    # async def test_server_exception_idempotent_retries(self):
    #     response = await self.balancing_client.get_url('test', '/')
    #     self.assertEqual(HTTPStatus.OK, response.response.code)

    @gen_test
    async def test_server_exception_non_idempotent_no_retry(self):
        response = await self.balancing_client.post_url('test', '/')
        self.assertEqual(599, response.response.code)
