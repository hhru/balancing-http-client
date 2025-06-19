import asyncio
import threading
from http import HTTPStatus

import aiohttp
import pytest
from pytest_httpserver import HTTPServer

from tests.test_balancing_base import BalancingClientMixin, TestBase


def low_backlog_server_handler(sock, event):
    sock.listen(1)
    event.wait()


class TestLowBacklog(TestBase, BalancingClientMixin):
    @pytest.fixture(scope='function', autouse=True)
    def setup_method(self, working_server: HTTPServer, setup_http_client_factory):
        self.low_backlog_server_socket, low_backlog_server_port = self.bind_unused_port()
        self.stop_event = threading.Event()
        low_backlog_server = threading.Thread(
            target=low_backlog_server_handler, args=(self.low_backlog_server_socket, self.stop_event)
        )
        low_backlog_server.daemon = True
        low_backlog_server.start()
        self.register_ports_for_upstream(low_backlog_server_port, working_server.port)

        loop = asyncio.new_event_loop()
        loop.run_until_complete(self.fill_backlog(low_backlog_server_port))
        loop.close()

    async def fill_backlog(self, port):
        http_client = aiohttp.ClientSession()
        timeout = aiohttp.ClientTimeout(total=0.1, connect=0.1)
        for i in range(3):
            try:
                await http_client.get(f'http://127.0.0.1:{port}', timeout=timeout)
            except Exception:
                pass
        await http_client.close()

    def teardown_method(self):
        self.stop_event.set()
        self.low_backlog_server_socket.close()

    async def test_low_backlog_idempotent_retries(self):
        result = await self.balancing_client.get_url('test', '/')
        assert result.status_code == HTTPStatus.OK

    async def test_low_backlog_non_idempotent_retries(self):
        result = await self.balancing_client.post_url('test', '/')
        assert result.status_code == HTTPStatus.OK
