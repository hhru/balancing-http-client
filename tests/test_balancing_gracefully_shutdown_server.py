import threading
from http import HTTPStatus

import pytest

from pytest_httpserver import HTTPServer
from tests.test_balancing_base import BalancingClientMixin, TestBase


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


class TestGracefulShutdown(TestBase, BalancingClientMixin):
    @pytest.fixture(scope="function", autouse=True)
    def setup_method(self, working_server: HTTPServer):
        self.gracefully_shutdown_server_socket, gracefully_shutdown_server_port = self.bind_unused_port()
        gracefully_server_thread = threading.Thread(target=gracefully_shutdown_server,
                                                    args=(self.gracefully_shutdown_server_socket,))
        gracefully_server_thread.daemon = True
        gracefully_server_thread.start()
        super().setup_method(working_server)
        self.register_ports_for_upstream(gracefully_shutdown_server_port, working_server.port)

    def teardown_method(self):
        self.gracefully_shutdown_server_socket.close()

    async def test_server_close_socket_idempotent_retries(self):
        result = await self.balancing_client.get_url('test', '/')
        assert result.status_code == HTTPStatus.OK

    async def test_server_close_socket_non_idempotent_no_retry(self):
        result = await self.balancing_client.post_url('test', '/')
        assert result.exc is not None
