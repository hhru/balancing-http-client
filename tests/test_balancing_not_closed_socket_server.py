import sys
import threading

import pytest

from pytest_httpserver import HTTPServer
from tests.test_balancing_base import BalancingClientMixin, TestBase


def not_closed_socket_server(sock):
    sock.listen(10)
    while True:
        try:
            client_sock, client_addr = sock.accept()
            client_sock.recv(4)
            raise Exception()
        except OSError:
            pass
        except Exception:
            return


class TestNotCloseSocket(TestBase, BalancingClientMixin):
    @pytest.fixture(scope="function", autouse=True)
    def setup_method(self, working_server: HTTPServer):
        self.not_closed_socket_server_socket, not_closed_socket_server_port = self.bind_unused_port()
        exceptionally_server_thread = threading.Thread(target=not_closed_socket_server,
                                                       args=(self.not_closed_socket_server_socket,))
        exceptionally_server_thread.daemon = True
        exceptionally_server_thread.start()
        super().setup_method(working_server)
        self.register_ports_for_upstream(not_closed_socket_server_port, working_server.port)

    def teardown_method(self):
        self.not_closed_socket_server_socket.close()

    # TODO doesn't work on macos,
    #  no signal after client_sock.recv(4), so there is asyncio.TimeoutError and no time for retry
    @pytest.mark.skipif(sys.platform == 'darwin', reason='problems with client_sock.recv on macos')
    async def test_server_exception_idempotent_retries(self):
        result = await self.balancing_client.get_url('test', '/')
        assert result.exc is None

    async def test_server_exception_non_idempotent_no_retry(self):
        result = await self.balancing_client.post_url('test', '/')
        assert result.exc is not None
