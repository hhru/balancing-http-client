import unittest

from http_client import options
from http_client.balancing import Upstream, Server


def _total_weight(upstream):
    return sum(server.weight for server in upstream.servers if server is not None)


def _total_requests(upstream):
    return sum(server.current_requests for server in upstream.servers if server is not None)


class TestHttpClientBalancer(unittest.TestCase):

    def setUp(self) -> None:
        super().setUp()
        options.datacenter = "test"

    @staticmethod
    def _upstream(servers, config=None):
        return Upstream('upstream', {} if config is None else config, servers)

    def test_create(self):
        upstream = self._upstream([Server('1', 1, dc='test'), Server('2', 1, dc='test')])

        self.assertEqual(len(upstream.servers), 2)
        self.assertEqual(_total_weight(upstream), 2)

    def test_add_server(self):
        servers = [Server('1', 1, dc='test'), Server('2', 1, dc='test')]
        upstream = self._upstream(servers)

        servers.append(Server('3', 1, dc='test'))
        upstream2 = self._upstream(servers)
        upstream.update(upstream2)

        self.assertEqual(len(upstream.servers), 3)
        self.assertEqual(_total_weight(upstream), 3)

    def test_replace_server(self):
        upstream = self._upstream([Server('1', 1, dc='test'), Server('2', 1, dc='test')])
        upstream2 = self._upstream([Server('1', 2, dc='test'), Server('2', 5, dc='test')])
        upstream.update(upstream2)

        self.assertEqual(len(upstream.servers), 2)
        self.assertEqual(_total_weight(upstream), 7)

    def test_remove_add_server(self):
        upstream = self._upstream([Server('1', 1, dc='test'), Server('2', 1, dc='test')])
        upstream2 = self._upstream([Server('2', 2, dc='test'), Server('3', 5, dc='test')])
        upstream.update(upstream2)

        self.assertEqual(len(upstream.servers), 2)
        self.assertEqual(_total_weight(upstream), 7)

    def test_remove_add_server_one_by_one(self):
        upstream = self._upstream([Server('1', 1, dc='test'), Server('2', 1, dc='test')])
        upstream2 = self._upstream([Server('2', 1, dc='test')])
        upstream.update(upstream2)

        self.assertEqual(len(upstream.servers), 2)
        self.assertEqual(_total_weight(upstream), 1)

        upstream2 = self._upstream([Server('2', 1, dc='test'), Server('3', 10, dc='test')])
        upstream.update(upstream2)

        self.assertEqual(len(upstream.servers), 2)
        self.assertEqual(_total_weight(upstream), 11)

    def test_acquire_server(self):
        upstream = self._upstream([Server('1', 2, dc='test'), Server('2', 1, dc='test')])

        address, _ = upstream.acquire_server()
        self.assertEqual(address, '1')

        address, _ = upstream.acquire_server()
        self.assertEqual(address, '2')

        address, _ = upstream.acquire_server()
        self.assertEqual(address, '1')

        address, _ = upstream.acquire_server()
        self.assertEqual(address, '1')

        self.assertEqual(_total_requests(upstream), 4)

    def test_acquire_release_server(self):
        upstream = self._upstream([Server('1', 1, dc='test'), Server('2', 5, dc='test')])

        address, _ = upstream.acquire_server()
        self.assertEqual(address, '1')

        address, _ = upstream.acquire_server()
        self.assertEqual(address, '2')

        upstream.release_server(address)

        address, _ = upstream.acquire_server()
        self.assertEqual(address, '2')

        self.assertEqual(_total_requests(upstream), 2)

    def test_release_none_server(self):
        upstream = self._upstream([Server('1', 1, dc='test')])
        upstream.servers[0] = None
        try:
            upstream.release_server('some_address')
        except AttributeError:
            self.fail('return server raised exception')

    def test_replace_in_process(self):
        upstream = self._upstream([Server('1', 1, dc='test'), Server('2', 5, dc='test')])

        address_1, _ = upstream.acquire_server()
        self.assertEqual(address_1, '1')

        address_2, _ = upstream.acquire_server()
        self.assertEqual(address_2, '2')

        server = Server('3', 1, dc='test')
        upstream2 = self._upstream([Server('1', 1, dc='test'), server])
        upstream.update(upstream2)

        self.assertEqual(_total_requests(upstream), 1)

        upstream.release_server(address_1)

        self.assertEqual(_total_requests(upstream), 0)
        self.assertEqual(server.current_requests, 0)

        address, _ = upstream.acquire_server()
        self.assertEqual(address, '1')

        address, _ = upstream.acquire_server()
        self.assertEqual(address, '3')

    def test_create_with_datacenter(self):
        upstream = self._upstream([Server('1', 1, dc='dc1'), Server('2', 1, dc='test')])

        self.assertEqual(len(upstream.servers), 2)
        self.assertEqual([server.datacenter for server in upstream.servers if server is not None], ['dc1', 'test'])

    def test_slow_start_on_server(self):
        servers = [Server('1', 1, dc='test'), Server('2', 1, dc='test')]
        upstream = Upstream('upstream', {'slow_start_interval_sec': 10}, servers)

        self.assertTrue(upstream.servers[0].slow_start_end_time > 0)
        self.assertTrue(upstream.servers[1].slow_start_end_time > 0)

    def test_session_required_true(self):
        servers = [Server('1', 1, dc='test'), Server('2', 1, dc='test')]
        upstream = Upstream('upstream', {'session_required': 'true'}, servers)

        self.assertEqual(upstream.session_required, 'true')

    def test_session_required_false(self):
        servers = [Server('1', 1, dc='test'), Server('2', 1, dc='test')]
        upstream = Upstream('upstream', {}, servers)

        self.assertEqual(upstream.session_required, False)
