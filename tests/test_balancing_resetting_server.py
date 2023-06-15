import socket
import struct
import threading
from http import HTTPStatus

from tornado.testing import bind_unused_port, gen_test

from tests.test_balancing_base import BalancingClientMixin, WorkingServerTestCase


def resetting_server(sock):
    sock.listen(10)
    client_sock = None
    while True:
        try:
            client_sock, client_addr = sock.accept()
            client_sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
            client_sock.recv(4)
        except BlockingIOError:
            pass
        finally:
            if client_sock:
                client_sock.close()


class ResettingServerTest(BalancingClientMixin, WorkingServerTestCase):

    def setUp(self):
        self.resetting_server_socket, resetting_server_port = bind_unused_port()
        resetting_server_thread = threading.Thread(target=resetting_server, args=(self.resetting_server_socket,))
        resetting_server_thread.daemon = True
        resetting_server_thread.start()
        super().setUp()
        self.register_ports_for_upstream(resetting_server_port, self.get_http_port())

    def tearDown(self):
        super().tearDown()
        self.resetting_server_socket.close()

    @gen_test
    async def test_server_reset_idempotent_retries(self):
        response = await self.balancing_client.get_url('test', '/')
        self.assertEqual(HTTPStatus.OK, response.response.code)

    # нельзя поретраить тк получаем ReadTimeout
    # @gen_test
    # async def test_server_reset_non_idempotent_no_retry(self):
    #     response = await self.balancing_client.post_url('test', '/')
    #     self.assertEqual(599, response.response.code)
