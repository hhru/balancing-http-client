from http_client import options
from http_client.balancing import Server, Upstream, UpstreamConfig


def _total_weight(upstream):
    return sum(server.weight for server in upstream.servers if server is not None)


def _total_requests(upstream):
    return sum(server.current_requests for server in upstream.servers if server is not None)


class TestHttpClientBalancer:
    @staticmethod
    def setup_method(self):
        options.datacenter = "test"

    @staticmethod
    def _upstream(servers, config=None):
        return Upstream('upstream', config, servers)

    def test_create(self):
        upstream = self._upstream([Server('1', "dest_host", 1, dc='test'), Server('2', "dest_host", 1, dc='test')])

        assert len(upstream.servers) == 2
        assert _total_weight(upstream) == 2

    def test_add_server(self):
        servers = [Server('1', "dest_host", 1, dc='test'), Server('2', "dest_host", 1, dc='test')]
        upstream = self._upstream(servers)

        servers.append(Server('3', "dest_host", 1, dc='test'))
        upstream2 = self._upstream(servers)
        upstream.update(upstream2)

        assert len(upstream.servers) == 3
        assert _total_weight(upstream) == 3

    def test_replace_server(self):
        upstream = self._upstream([Server('1', "dest_host", 1, dc='test'), Server('2', "dest_host", 1, dc='test')])
        upstream2 = self._upstream([Server('1', "dest_host", 2, dc='test'), Server('2', "dest_host", 5, dc='test')])
        upstream.update(upstream2)

        assert len(upstream.servers) == 2
        assert _total_weight(upstream) == 7

    def test_remove_add_server(self):
        upstream = self._upstream([Server('1', "dest_host", 1, dc='test'), Server('2', "dest_host", 1, dc='test')])
        upstream2 = self._upstream([Server('2', "dest_host", 2, dc='test'), Server('3', "dest_host", 5, dc='test')])
        upstream.update(upstream2)

        assert len(upstream.servers) == 2
        assert _total_weight(upstream) == 7

    def test_remove_add_server_one_by_one(self):
        upstream = self._upstream([Server('1', "dest_host", 1, dc='test'), Server('2', "dest_host", 1, dc='test')])
        upstream2 = self._upstream([Server('2', "dest_host", 1, dc='test')])
        upstream.update(upstream2)

        assert len(upstream.servers) == 2
        assert _total_weight(upstream) == 1

        upstream2 = self._upstream([Server('2', "dest_host", 1, dc='test'), Server('3', "dest_host", 10, dc='test')])
        upstream.update(upstream2)

        assert len(upstream.servers) == 2
        assert _total_weight(upstream) == 11

    def test_dest_host(self):
        upstream = self._upstream([Server('2', "dest_host_1", 1, dc='test')])

        address, dc_result, dest_host_result  = upstream.acquire_server()
        assert address == '2'
        assert dc_result == 'test'
        assert dest_host_result == 'dest_host_1'

    def test_acquire_server(self):
        upstream = self._upstream([Server('1', "dest_host", 2, dc='test'), Server('2', "dest_host", 1, dc='test')])

        address, dest_host, _ = upstream.acquire_server()
        assert address == '1'
        print(address)
        print(dest_host)
        print(_)

        address, dest_host, _ = upstream.acquire_server()
        assert address == '2'

        address, dest_host, _ = upstream.acquire_server()
        assert address == '1'

        address, dest_host, _ = upstream.acquire_server()
        assert address == '1'

        assert _total_requests(upstream) == 4

    def test_acquire_release_server(self):
        upstream = self._upstream([Server('1', "dest_host", 1, dc='test'), Server('2', "dest_host", 5, dc='test')])

        address, dest_host, _ = upstream.acquire_server()
        assert address == '1'

        address, dest_host, _ = upstream.acquire_server()
        assert address == '2'

        upstream.release_server(address, False, 0, False)

        address, dest_host, _ = upstream.acquire_server()
        assert address == '2'

        assert _total_requests(upstream) == 2

    def test_release_none_server(self):
        upstream = self._upstream([Server('1', "dest_host", 1, dc='test')])
        upstream.servers[0] = None
        upstream.release_server('some_address', False, 0, False)

    def test_replace_in_process(self):
        upstream = self._upstream([Server('1', "dest_host", 1, dc='test'), Server('2', "dest_host", 5, dc='test')])

        address_1, dest_host, _ = upstream.acquire_server()
        assert address_1 == '1'

        address_2, dest_host, _ = upstream.acquire_server()
        assert address_2 == '2'

        server = Server('3', "dest_host", 1, dc='test')
        upstream2 = self._upstream([Server('1', "dest_host", 1, dc='test'), server])
        upstream.update(upstream2)

        assert _total_requests(upstream) == 1

        upstream.release_server(address_1, False, 0, False)

        assert _total_requests(upstream) == 0
        assert server.current_requests == 0

        address, dest_host, _ = upstream.acquire_server()
        assert address == '1'

        address, dest_host, _ = upstream.acquire_server()
        assert address == '3'

    def test_create_with_datacenter(self):
        upstream = self._upstream([Server('1', "dest_host", 1, dc='dc1'), Server('2', "dest_host", 1, dc='test')])

        assert len(upstream.servers) == 2
        assert [server.datacenter for server in upstream.servers if server is not None] == ['dc1', 'test']

    def test_slow_start_on_server(self):
        servers = [Server('1', "dest_host", 1, dc='test'), Server('2', "dest_host", 1, dc='test')]
        upstream_config = {Upstream.DEFAULT_PROFILE: UpstreamConfig(slow_start_interval=10)}
        upstream = Upstream('upstream', upstream_config, servers)

        assert upstream.servers[0].slow_start_end_time > 0
        assert upstream.servers[1].slow_start_end_time > 0

    def test_session_required_true(self):
        servers = [Server('1', "dest_host", 1, dc='test'), Server('2', "dest_host", 1, dc='test')]
        upstream_config = {Upstream.DEFAULT_PROFILE: UpstreamConfig(session_required=True)}
        upstream = Upstream('upstream', upstream_config, servers)

        assert upstream.get_config(Upstream.DEFAULT_PROFILE).session_required is True

    def test_session_required_false(self):
        servers = [Server('1', "dest_host", 1, dc='test'), Server('2', "dest_host", 1, dc='test')]
        upstream_config = {Upstream.DEFAULT_PROFILE: UpstreamConfig()}
        upstream = Upstream('upstream', upstream_config, servers)

        assert upstream.get_config(Upstream.DEFAULT_PROFILE).session_required is False
