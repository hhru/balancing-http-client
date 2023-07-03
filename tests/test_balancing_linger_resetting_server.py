import socket
import struct
import threading
import time
from http import HTTPStatus

import pytest

from pytest_httpserver import HTTPServer
from tests.test_balancing_base import BalancingClientMixin, TestBase


def linger_resetting_server(sock):
    sock.listen(10)
    client_sock = None
    while True:
        try:
            client_sock, client_addr = sock.accept()
            client_sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
            time.sleep(0.1)
            client_sock.recv(1024)  # got whole request
        except (BlockingIOError, OSError):
            pass
        finally:
            if client_sock:
                client_sock.close()


class TestLingerResettingServer(TestBase, BalancingClientMixin):
    @pytest.fixture(scope="function", autouse=True)
    def setup_method(self, working_server: HTTPServer):
        self.resetting_server_socket, resetting_server_port = self.bind_unused_port()
        resetting_server_thread = threading.Thread(target=linger_resetting_server, args=(self.resetting_server_socket,))
        resetting_server_thread.daemon = True
        resetting_server_thread.start()
        super().setup_method(working_server)
        self.register_ports_for_upstream(resetting_server_port, working_server.port)

    def teardown_method(self):
        self.resetting_server_socket.close()

    async def test_server_reset_idempotent_retries(self):
        result = await self.balancing_client.get_url('test', '/')
        assert result.status_code == HTTPStatus.OK

    async def test_server_reset_non_idempotent_no_retry(self):
        result = await self.balancing_client.post_url('test', '/')
        assert result.exc is not None
