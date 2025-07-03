import asyncio
import random
import timeit
from functools import partial

import pytest
import yarl
from aiohttp.client_exceptions import ServerTimeoutError
from aiohttp.client_reqrep import ClientResponse

from http_client.balancing import (
    RESPONSE_TIME_TRACKER_WINDOW,
    AdaptiveBalancingState,
    AdaptiveBalancingStrategy,
    BalancingState,
    Server,
    Upstream,
    UpstreamConfigs,
)
from http_client.request_response import RequestBuilder, RequestResult


class TestAdaptiveBalancingStrategy:
    def test_should_pick_less_than_all(self):
        servers = generate_servers(3)
        retries_count = 2
        balanced_servers = AdaptiveBalancingStrategy.get_servers(servers, retries_count)
        assert len(balanced_servers) == retries_count

    def test_should_pick_different(self):
        servers = generate_servers(3)
        balanced_servers = AdaptiveBalancingStrategy.get_servers(servers, len(servers))
        assert sorted(list(map(lambda s: s.address, balanced_servers))) == ['test1', 'test2', 'test3']

    def test_should_pick_same_server_several_times(self):
        servers = generate_servers(1)
        balanced_servers = AdaptiveBalancingStrategy.get_servers(servers, 3)
        assert list(map(lambda s: s.address, balanced_servers)) == ['test1', 'test1', 'test1']

    def test_should_pick_as_much_as_requested(self):
        servers = generate_servers(3)
        balanced_servers = AdaptiveBalancingStrategy.get_servers(servers, 5)

        assert len(balanced_servers) == 5
        addresses = list(map(lambda s: s.address, balanced_servers))
        assert addresses[0:2] == addresses[3:5], 'Extra servers should be repeated in the same order'

    def test_should_return_empty(self):
        assert AdaptiveBalancingStrategy.get_servers(generate_servers(3), 0) == []
        assert AdaptiveBalancingStrategy.get_servers([], 2) == []

    def test_should_warm_up(self):
        servers = generate_servers(2)
        retries_count = 2
        balanced_servers = AdaptiveBalancingStrategy.get_servers(servers, retries_count)

        response_time_tracker1 = balanced_servers[0].response_time_tracker
        response_time_tracker2 = balanced_servers[1].response_time_tracker
        assert response_time_tracker1.is_warm_up
        assert response_time_tracker2.is_warm_up

        for _ in range(RESPONSE_TIME_TRACKER_WINDOW):
            response_time_tracker1.add_response_time(random.randrange(100, 200))

        balanced_servers = AdaptiveBalancingStrategy.get_servers(servers, retries_count)
        warm_up1 = balanced_servers[0].response_time_tracker.is_warm_up
        warm_up2 = balanced_servers[1].response_time_tracker.is_warm_up
        assert warm_up1 != warm_up2, 'Only one server should be warmed up'

    def test_same_load(self):
        random.seed(123456)
        servers = generate_servers(3)
        upstream = Upstream('my_backend', UpstreamConfigs({}), servers)
        upstream.datacenter = 'dc1'

        def response_time_func(host):
            return 0.1

        def response_type_func(host):
            return 200

        server_statistics = make_requests(upstream, response_time_func, response_type_func)
        # should be nearly 1:1:1 randomly-distributed between all three servers (~0.33)
        assert 0.31 <= server_statistics['test1']['rate'] <= 0.35
        assert 0.31 <= server_statistics['test2']['rate'] <= 0.35
        assert 0.31 <= server_statistics['test3']['rate'] <= 0.35

    def test_one_slow_server(self):
        random.seed(123456)
        servers = generate_servers(3)
        upstream = Upstream('my_backend', UpstreamConfigs({}), servers)
        upstream.datacenter = 'dc1'

        def response_time_func(host):
            if host == 'test3':
                return 0.2
            else:
                return 0.1

        def response_type_func(host):
            return 200

        server_statistics = make_requests(upstream, response_time_func, response_type_func)
        # should be nearly 2:2:1 (actually 39:39:22 because of time tracker window) randomly-distributed
        # between test1, test2, test3 (~0.39, ~0.39, ~0.22)
        assert 0.37 <= server_statistics['test1']['rate'] <= 0.41
        assert 0.37 <= server_statistics['test2']['rate'] <= 0.41
        assert 0.20 <= server_statistics['test3']['rate'] <= 0.24

    def test_one_fail_server(self):
        random.seed(123456)
        servers = generate_servers(3)
        upstream = Upstream('my_backend', UpstreamConfigs({}), servers)
        upstream.datacenter = 'dc1'

        def response_time_func(host):
            return 0.1

        def response_type_func(host):
            if host == 'test3':
                return 503
            else:
                return 200

        server_statistics = make_requests(upstream, response_time_func, response_type_func)
        # should be nearly 1:1 randomly-distributed between test1 and test2 (~0.50)
        assert 0.48 <= server_statistics['test1']['rate'] <= 0.52
        assert 0.48 <= server_statistics['test2']['rate'] <= 0.52
        assert server_statistics['test3']['rate'] == 0

    def test_one_restarted_server(self):
        random.seed(123456)
        servers = generate_servers(3)
        upstream = Upstream('my_backend', UpstreamConfigs({}), servers)
        upstream.datacenter = 'dc1'

        def response_time_func(host):
            return 0.1

        counter = 0

        def response_type_func(host):
            if host == 'test3':
                nonlocal counter
                counter += 1
                if 1000 <= counter < 1100:
                    return ServerTimeoutError
            return 200

        server_statistics = make_requests(upstream, response_time_func, response_type_func)
        # should be randomly-distributed between test1, test2, test3 at about ~0.365, ~0.365, ~0.27
        assert 0.345 <= server_statistics['test1']['rate'] <= 0.385
        assert 0.345 <= server_statistics['test2']['rate'] <= 0.385
        assert 0.25 <= server_statistics['test3']['rate'] <= 0.29

    @pytest.mark.skip(reason='for dev purpose')
    def test_speed(self):
        servers = generate_servers(7)
        upstream = Upstream('my_backend', UpstreamConfigs({}), servers)
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


def generate_servers(n: int) -> list[Server]:
    return [Server(f'test{i}', 'destHost', 1, 'dc1') for i in range(1, n + 1)]


def make_requests(upstream, response_time_func, response_type_func):
    servers_hits = {server.address: {'ok': 0, 'fail': 0} for server in upstream.servers}
    n_requests = 10_000
    for i in range(n_requests):
        state = AdaptiveBalancingState(upstream, 'default')
        upstream_config = state.get_upstream_config()
        max_tries = upstream_config.max_tries
        execute_request_with_retry(state, max_tries, servers_hits, response_time_func, response_type_func)

    for server in servers_hits:
        servers_hits[server]['rate'] = servers_hits[server]['ok'] / n_requests
    return servers_hits


def execute_request_with_retry(state, tries_left, servers_hits, response_time_func, response_type_func):
    # 1. получаем сервер на который пойдем
    state.acquire_server()
    if not state.is_server_available():
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
    upstream_config = state.get_upstream_config()
    is_server_error = upstream_config.retry_policy.is_server_error(result)
    state.release_server(result.elapsed_time, is_server_error)

    if result.failed and tries_left > 0:
        state.increment_tries()
        execute_request_with_retry(state, tries_left - 1, servers_hits, response_time_func, response_type_func)


def make_result_object(request, response_time_s, response_type) -> RequestResult:
    if isinstance(response_type, int):
        url = yarl.URL(request.url)
        client_response = ClientResponse(
            request.method,
            url,
            writer=None,
            continue100=None,
            timer=None,
            request_info=None,
            traces=None,
            loop=asyncio.get_event_loop(),
            session=None,
        )
        client_response.status = response_type
        result = RequestResult(
            request,
            client_response.status,
            response=client_response,
            elapsed_time=response_time_s,
            parse_response=False,
        )
    else:
        exc = response_type('request failed')
        result = RequestResult(request, 599, elapsed_time=response_time_s, exc=exc, parse_response=False)

    return result
