import json

from http_client.balancing import BalancingStrategyType, Upstream
from http_client.options import options
from http_client.parsing.consul_parser import parse_consul_health_servers_data, parse_consul_upstream_config


class TestParser:
    def test_parse_config(self) -> None:
        value = {
            'Value': json.dumps({
                'hosts': {
                    'default': {
                        'profiles': {
                            'default': {
                                'max_tries': '3',
                                'request_timeout_sec': '5',
                                'connect_timeout_sec': '0.2',
                                'max_timeout_tries': '1',
                                'slow_start_interval_sec': '150',
                                'speculative_timeout_pct': '0.5',
                                'session_required': 'true',
                            },
                            'slow': {
                                'max_tries': '6',
                                'request_timeout_sec': '10',
                                'connect_timeout_sec': '0.5',
                                'max_timeout_tries': '2',
                                'slow_start_interval_sec': '300',
                                'speculative_timeout_pct': '0.7',
                                'session_required': 'false',
                            },
                        },
                    },
                },
                'balancing_strategy': 'adaptive',
            })
        }
        configs = parse_consul_upstream_config(value)
        assert configs.balancing_strategy_type == BalancingStrategyType.ADAPTIVE

        config = configs.config_by_profile

        assert len(config) == 2

        default_config = config.get(Upstream.DEFAULT_PROFILE)

        assert default_config is not None
        assert default_config.max_tries == 3
        assert default_config.request_timeout == 5
        assert default_config.connect_timeout == 0.2
        assert default_config.max_timeout_tries == 1
        assert default_config.slow_start_interval == 150
        assert default_config.speculative_timeout_pct == 0.5
        assert default_config.session_required is True

        slow_config = config.get('slow')

        assert slow_config is not None
        assert slow_config.max_tries == 6
        assert slow_config.request_timeout == 10
        assert slow_config.connect_timeout == 0.5
        assert slow_config.max_timeout_tries == 2
        assert slow_config.slow_start_interval == 300
        assert slow_config.speculative_timeout_pct == 0.7
        assert slow_config.session_required is False

    def test_parse_config_default_value(self) -> None:
        value = {'Value': json.dumps({'hosts': {'default': {'profiles': {'default': {'max_tries': '3'}}}}})}
        configs = parse_consul_upstream_config(value)
        config = configs.config_by_profile

        default_config = config.get(Upstream.DEFAULT_PROFILE)

        assert default_config is not None
        assert default_config.max_tries == 3
        assert default_config.session_required == options.http_client_default_session_required
        assert default_config.speculative_timeout_pct == 0

    def test_parse_health_service(self) -> None:
        value = [
            {
                'Node': {
                    'ID': '1',
                    'Node': '',
                    'Address': '1.1.1.1',
                    'Datacenter': 'test',
                },
                'Service': {
                    'ID': '2',
                    'Service': 'app',
                    'Address': '',
                    'Port': 9999,
                    'Weights': {'Passing': 100, 'Warning': 0},
                },
            }
        ]

        _, servers = parse_consul_health_servers_data(value)

        assert len(servers) == 1
        assert servers[0].address == '1.1.1.1:9999'
        assert servers[0].weight == 100
        assert servers[0].datacenter == 'test'

    def test_parse_health_service_not_test_datacenter(self) -> None:
        value = [
            {
                'Node': {
                    'ID': '1',
                    'Node': 'some_name',
                    'Address': '1.1.1.1',
                    'Datacenter': 'test',
                },
                'Service': {
                    'ID': '2',
                    'Service': 'app',
                    'Address': '',
                    'Port': 9999,
                    'Weights': {'Passing': 100, 'Warning': 0},
                },
            }
        ]

        _, servers = parse_consul_health_servers_data(value)

        assert len(servers) == 1

    def test_parse_health_service_not_test_datacenter_with_self_enabled_filter(self) -> None:
        options.self_node_filter_enabled = True
        value = [
            {
                'Node': {
                    'ID': '1',
                    'Node': 'some_name',
                    'Address': '1.1.1.1',
                    'Datacenter': 'test',
                },
                'Service': {
                    'ID': '2',
                    'Service': 'app',
                    'Address': '',
                    'Port': 9999,
                    'Weights': {'Passing': 100, 'Warning': 0},
                },
            }
        ]

        _, servers = parse_consul_health_servers_data(value)

        assert len(servers) == 0

    def test_parse_retry_policy(self) -> None:
        value = {
            'Value': """{
                "hosts":{
                    "default":{
                        "profiles":{
                            "default":{
                                "max_timeout_tries":"2",
                                "request_timeout_sec":"2",
                                "connect_timeout_sec":"0.1",
                                "retry_policy":{
                                    "503":{
                                        "retry_non_idempotent":"true"
                                    },
                                    "599":{
                                        "retry_non_idempotent":"false"
                                    },
                                    "502":{
                                        "idempotent":"true"
                                    },
                                    "504":{
                                        "idempotent":"false",
                                        "retry_non_idempotent":"true"
                                    }
                                },
                                "max_tries":"3"
                            },
                            "without_retry_policy":{
                            },
                            "with_empty_retry_policy":{
                                "retry_policy":{
                                }
                            }
                        }
                    }
                }
            }"""
        }

        configs = parse_consul_upstream_config(value)
        upstream = Upstream('some_upstream', configs, [])

        assert upstream.get_config(Upstream.DEFAULT_PROFILE).retry_policy.statuses == {
            503: True,
            599: False,
            502: False,
            504: True,
        }

        assert (
            upstream.get_config('without_retry_policy').retry_policy.statuses
            == options.http_client_default_retry_policy
        )

        assert (
            upstream.get_config('with_empty_retry_policy').retry_policy.statuses
            == options.http_client_default_retry_policy
        )

    def test_balancing_strategy_parsing(self) -> None:
        self._test_balancing_strategy_parsing('weighted', BalancingStrategyType.WEIGHTED)
        self._test_balancing_strategy_parsing('adaptive', BalancingStrategyType.ADAPTIVE)

        # we accept only lower case -> fallback to default
        self._test_balancing_strategy_parsing('ADAPTIVE', BalancingStrategyType.WEIGHTED)

        # unknown value -> fallback to default
        self._test_balancing_strategy_parsing('foo_asd', BalancingStrategyType.WEIGHTED)

        # empty value is also unknown -> fallback to default
        self._test_balancing_strategy_parsing('', BalancingStrategyType.WEIGHTED)

    @staticmethod
    def _test_balancing_strategy_parsing(
        balancing_strategy: str,
        expected_balancing_strategy_type: BalancingStrategyType,
    ) -> None:
        value = (
            """{
                "balancing_strategy": "%s",
                "hosts": {
                    "default": {
                        "profiles": {
                            "default": {
                            }
                        }
                    }
                }
            }"""  # noqa: UP031
            % balancing_strategy
        )

        configs = parse_consul_upstream_config({'Value': value})
        assert configs.balancing_strategy_type == expected_balancing_strategy_type

    def test_missing_balancing_strategy_parsing(self) -> None:
        value = """{
            "hosts": {
                "default": {
                    "profiles": {
                        "default": {
                        }
                    }
                }
            }
        }"""

        configs = parse_consul_upstream_config({'Value': value})

        # balancing strategy not specified -> use default
        assert configs.balancing_strategy_type == BalancingStrategyType.WEIGHTED
