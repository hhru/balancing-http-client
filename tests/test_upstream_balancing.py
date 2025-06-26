import random

from http_client import options
from http_client.balancing import Server, Upstream, UpstreamConfigs


def run_simulation(upstream, requests_interval, requests, max_execution_time):
    timeline = []
    mapping = {}
    done = [0] * len(upstream.servers)

    for i in range(requests_interval + max_execution_time + 1):
        timeline.append([])

    for i in range(requests):
        start_time = random.randint(0, requests_interval)
        timeline[start_time].append((i, True))
        request_time = random.randint(1, max_execution_time)
        timeline[start_time + request_time].append((i, False))

    for commands in timeline:
        for index, acquire in commands:
            if acquire:
                address, _, dest_host = upstream.acquire_server()
                address_index = next(
                    (i for i, server in enumerate(upstream.servers) if server.address == address), None
                )
                done[address_index] = done[address_index] + 1
                mapping[index] = address_index
            else:
                upstream.release_server([upstream.servers[mapping[index]].address], False, 0, False)
                del mapping[index]
    return done


def _upstream(weights):
    return Upstream(
        'upstream',
        UpstreamConfigs({}),
        [Server(str(weight), hostname='dest_host', weight=weight, dc='test') for weight in weights],
    )


class TestHttpError:
    def setUp(self) -> None:
        options.datacenter = 'test'

    def check_distribution(self, requests, weights):
        if len(requests) != len(weights) or len(requests) <= 1:
            raise ValueError(f'invalid input data: {requests}, {weights}')

        for i in range(1, len(requests)):
            request_ratio = float(requests[i]) / float(requests[i - 1])
            weights_ratio = float(weights[i]) / float(weights[i - 1])

            assert abs(request_ratio - weights_ratio) <= 0.3, (
                f'{requests} and {weights} ratio difference for elements {i - 1} and {i} '
                f'is too big: {request_ratio} vs {weights_ratio}'
            )

    def test_sparse_requests(self):
        weights = [50, 100, 200]
        requests = run_simulation(_upstream(weights), requests_interval=10000, requests=3500, max_execution_time=200)

        self.check_distribution(requests, weights)

    def test_dense_requests(self):
        weights = [50, 100, 200]
        requests = run_simulation(_upstream(weights), requests_interval=10000, requests=100000, max_execution_time=1000)

        self.check_distribution(requests, weights)

    def test_short_execution_time(self):
        weights = [50, 100, 200]
        requests = run_simulation(_upstream(weights), requests_interval=800, requests=3500, max_execution_time=10)

        self.check_distribution(requests, weights)

    def test_long_execution_time(self):
        weights = [50, 100, 200]
        requests = run_simulation(_upstream(weights), requests_interval=10000, requests=3500, max_execution_time=10000)

        self.check_distribution(requests, weights)
