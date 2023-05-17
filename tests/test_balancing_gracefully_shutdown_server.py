import threading
from http import HTTPStatus

from tornado.testing import gen_test, bind_unused_port

from tests.test_balancing_base import BalancingClientMixin, WorkingServerTestCase


def gracefully_shutdown_server(sock):
    sock.listen(10)
    client_sock = None
    while True:
        try:
            client_sock, client_addr = sock.accept()
            client_sock.recv(4)
        except OSError:
            pass
        finally:
            if client_sock:
                client_sock.close()


class GracefulShutdownTest(BalancingClientMixin, WorkingServerTestCase):

    def setUp(self):
        self.gracefully_shutdown_server_socket, gracefully_shutdown_server_port = bind_unused_port()
        gracefully_server_thread = threading.Thread(target=gracefully_shutdown_server,
                                                    args=(self.gracefully_shutdown_server_socket,))
        gracefully_server_thread.daemon = True
        gracefully_server_thread.start()
        super().setUp()
        self.register_ports_for_upstream(gracefully_shutdown_server_port, self.get_http_port())

    def tearDown(self):
        super().tearDown()
        self.gracefully_shutdown_server_socket.close()

    @gen_test
    async def test_server_close_socket_idempotent_retries(self):
        response = await self.balancing_client.get_url('test', '/')
        self.assertEqual(HTTPStatus.OK, response.response.code)

    @gen_test
    async def test_server_close_socket_non_idempotent_no_retry(self):
        response = await self.balancing_client.post_url('test', '/')
        self.assertEqual(599, response.response.code)
