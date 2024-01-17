from http_client.balancing import Server, AdaptiveBalancingStrategy, AdaptiveBalancingState, Upstream, BalancingState
from http_client.request_response import RequestResult, RequestBuilder
from aiohttp.client_reqrep import ClientResponse
from aiohttp.client_exceptions import ServerTimeoutError
import asyncio
import yarl
import timeit
from functools import partial
import random
import pytest


class TestAdaptiveBalancingStrategy:
    def test_should_pick_less_than_all(self):
        servers = [Server("test1", 1, None), Server("test2", 1, None), Server("test3", 1, None)]
        retries_count = 2
        balanced_servers = AdaptiveBalancingStrategy.get_servers(servers, retries_count)
        assert len(balanced_servers) == retries_count

    def test_should_pick_different(self):
        servers = [Server("test1", 1, None), Server("test2", 1, None), Server("test3", 1, None)]
        balanced_servers = AdaptiveBalancingStrategy.get_servers(servers, len(servers))
        assert ['test1', 'test2', 'test3'] == sorted(list(map(lambda s: s.address, balanced_servers)))

    def test_same_load(self):
        servers = [Server('test1', 1, 'dc1'), Server('test2', 1, 'dc1'), Server('test3', 1, 'dc1')]
        upstream = Upstream('my_backend', {}, servers)
        upstream.datacenter = 'dc1'

        def response_time_func(host):
            return 0.1

        def response_type_func(host):
            return 200

        server_statistics = make_requests(upstream, response_time_func, response_type_func)
        assert 0.31 <= server_statistics['test1']['rate'] <= 0.35
        assert 0.31 <= server_statistics['test2']['rate'] <= 0.35
        assert 0.31 <= server_statistics['test3']['rate'] <= 0.35

    def test_one_slow_server(self):
        servers = [Server('test1', 1, 'dc1'), Server('test2', 1, 'dc1'), Server('test3', 1, 'dc1')]
        upstream = Upstream('my_backend', {}, servers)
        upstream.datacenter = 'dc1'

        def response_time_func(host):
            if host == 'test3':
                return 0.2
            else:
                return 0.1

        def response_type_func(host):
            return 200

        server_statistics = make_requests(upstream, response_time_func, response_type_func)
        assert 0.37 <= server_statistics['test1']['rate'] <= 0.41
        assert 0.37 <= server_statistics['test2']['rate'] <= 0.41
        assert 0.20 <= server_statistics['test3']['rate'] <= 0.24

    def test_one_fail_server(self):
        servers = [Server('test1', 1, 'dc1'), Server('test2', 1, 'dc1'), Server('test3', 1, 'dc1')]
        upstream = Upstream('my_backend', {}, servers)
        upstream.datacenter = 'dc1'

        def response_time_func(host):
            return 0.1

        def response_type_func(host):
            if host == 'test3':
                return 503
            else:
                return 200

        server_statistics = make_requests(upstream, response_time_func, response_type_func)
        assert 0.49 <= server_statistics['test1']['rate'] <= 0.51
        assert 0.49 <= server_statistics['test2']['rate'] <= 0.51
        assert 0.0 <= server_statistics['test3']['rate'] <= 0.01

    def test_one_restarted_server(self):
        servers = [Server('test1', 1, 'dc1'), Server('test2', 1, 'dc1'), Server('test3', 1, 'dc1')]
        upstream = Upstream('my_backend', {}, servers)
        upstream.datacenter = 'dc1'

        def response_time_func(host):
            return 0.1

        counter = 0

        def response_type_func(host):
            if host == 'test3':
                nonlocal counter
                counter += 1
                if counter < 1000:
                    return 200
                elif counter < 1100:
                    return ServerTimeoutError
                else:
                    return 200
            else:
                return 200

        server_statistics = make_requests(upstream, response_time_func, response_type_func)
        assert 0.33 <= server_statistics['test1']['rate'] <= 0.38
        assert 0.33 <= server_statistics['test2']['rate'] <= 0.38
        assert 0.25 <= server_statistics['test3']['rate'] <= 0.30

    @pytest.mark.skip(reason='for dev purpose')
    def test_speed(self):
        servers = [Server('test1', 1, 'dc1'), Server('test2', 1, 'dc1'), Server('test3', 1, 'dc1'),
                   Server('test4', 1, 'dc1'), Server('test5', 1, 'dc1'), Server('test6', 1, 'dc1'),
                   Server('test7', 1, 'dc1')]
        upstream = Upstream('my_backend', {}, servers)
        upstream.datacenter = 'dc1'
        servers_hits = {server.address: {'ok': 0, 'fail': 0} for server in upstream.servers}

        def make_many_requests(adaptive=False):
            n_requests = 100_000

            def response_time_func(host):
                x = random.random()
                return x * 0.8 + 0.1

            def response_type_func(host):
                return 200

            for i in range(n_requests):
                state = AdaptiveBalancingState(upstream, 'default') if adaptive else BalancingState(upstream, 'default')
                upstream_config = state.get_upstream_config()
                max_tries = upstream_config.max_tries
                execute_request_with_retry(state, max_tries, servers_hits, response_time_func, response_type_func)

        print('report:')
        res = timeit.timeit(partial(make_many_requests, True), number=1)
        print(f'adaptive - {res} sec')
        res = timeit.timeit(make_many_requests, number=1)
        print(f'simple - {res} sec')


def make_requests(upstream, response_time_func, response_type_func):
    servers_hits = {server.address: {'ok': 0, 'fail': 0} for server in upstream.servers}
    n_requests = 10_000
    for i in range(n_requests):
        state = AdaptiveBalancingState(upstream, 'default')
        upstream_config = state.get_upstream_config()
        max_tries = upstream_config.max_tries
        execute_request_with_retry(state, max_tries, servers_hits, response_time_func, response_type_func)

    for server in servers_hits.keys():
        servers_hits[server]['rate'] = servers_hits[server]['ok'] / n_requests
    return servers_hits


def execute_request_with_retry(state, tries_left, servers_hits, response_time_func, response_type_func):
    # 1. получаем сервер на который пойдем
    state.acquire_server()
    if not state.is_server_available() or (isinstance(state, AdaptiveBalancingState) and state.adaptive_failed):
        raise Exception('no available server')
    host = state.current_host

    # 2. генерим реквест/резалт
    request = RequestBuilder(host, 'source_app', 'path', 'name')
    response_time = response_time_func(host)
    response_type = response_type_func(host)
    result = make_result_object(request, response_time, response_type)
    if not result.failed:
        servers_hits[host]['ok'] += 1
    else:
        servers_hits[host]['fail'] += 1

    # 3. on_response
    upstream_config = state.upstream.get_config(state.profile)
    is_server_error = upstream_config.retry_policy.is_server_error(result)
    state.release_server(result.elapsed_time, is_server_error)

    if result.failed and tries_left > 0:
        state.increment_tries()
        execute_request_with_retry(state, tries_left - 1, servers_hits, response_time_func, response_type_func)


def make_result_object(request, response_time_s, response_type) -> RequestResult:
    if isinstance(response_type, int):
        url = yarl.URL(request.url)
        client_response = ClientResponse(request.method, url, writer=None, continue100=None, timer=None,
                                         request_info=None, traces=None, loop=asyncio.get_event_loop(),
                                         session=None)
        client_response.status = response_type
        result = RequestResult(request, client_response.status, response=client_response, elapsed_time=response_time_s,
                               parse_response=False)
    else:
        exc = response_type('request failed')
        result = RequestResult(request, 599, elapsed_time=response_time_s, exc=exc, parse_response=False)

    return result
