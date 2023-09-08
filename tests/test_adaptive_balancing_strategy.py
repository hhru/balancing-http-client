from http_client.balancing import Server, AdaptiveBalancingStrategy


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
