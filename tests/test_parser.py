import json
import unittest

from http_client.balancing import Upstream
from http_client.options import options
from http_client.consul_parser import parse_consul_upstream_config, parse_consul_health_servers_data


class TestParser(unittest.TestCase):

    def test_parse_config(self):
        value = {'Value': json.dumps(
            {
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
                            }
                        }
                    }
                }
            }
        )}
        config = parse_consul_upstream_config(value)

        self.assertEqual(len(config), 2)

        self.assertEqual(config.get(Upstream.DEFAULT_PROFILE).max_tries, 3)
        self.assertEqual(config.get(Upstream.DEFAULT_PROFILE).request_timeout, 5)
        self.assertEqual(config.get(Upstream.DEFAULT_PROFILE).connect_timeout, 0.2)
        self.assertEqual(config.get(Upstream.DEFAULT_PROFILE).max_timeout_tries, 1)
        self.assertEqual(config.get(Upstream.DEFAULT_PROFILE).slow_start_interval, 150)
        self.assertEqual(config.get(Upstream.DEFAULT_PROFILE).speculative_timeout_pct, 0.5)
        self.assertEqual(config.get(Upstream.DEFAULT_PROFILE).session_required, True)

        self.assertEqual(config.get("slow").max_tries, 6)
        self.assertEqual(config.get("slow").request_timeout, 10)
        self.assertEqual(config.get("slow").connect_timeout, 0.5)
        self.assertEqual(config.get("slow").max_timeout_tries, 2)
        self.assertEqual(config.get("slow").slow_start_interval, 300)
        self.assertEqual(config.get("slow").speculative_timeout_pct, 0.7)
        self.assertEqual(config.get("slow").session_required, False)

    def test_parse_config_default_value(self):
        value = {'Value': json.dumps(
            {
                'hosts': {
                    'default': {
                        'profiles': {
                            'default': {
                                'max_tries': '3'
                            }
                        }
                    }
                }
            }
        )}
        config = parse_consul_upstream_config(value)

        self.assertEqual(config.get(Upstream.DEFAULT_PROFILE).max_tries, 3)
        self.assertEqual(config.get(Upstream.DEFAULT_PROFILE).session_required,
                         options.http_client_default_session_required)
        self.assertEqual(config.get(Upstream.DEFAULT_PROFILE).speculative_timeout_pct, 0)

    def test_parse_health_service(self):
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
                    'Weights': {
                        'Passing': 100,
                        'Warning': 0
                    }
                }
            }
        ]

        dc, servers = parse_consul_health_servers_data(value)

        self.assertEqual(len(servers), 1)
        self.assertEqual(servers[0].address, '1.1.1.1:9999')
        self.assertEqual(servers[0].weight, 100)
        self.assertEqual(servers[0].datacenter, 'test')

    def test_parse_health_service_not_test_datacenter(self):
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
                    'Weights': {
                        'Passing': 100,
                        'Warning': 0
                    }
                }
            }
        ]

        dc, servers = parse_consul_health_servers_data(value)

        self.assertEqual(len(servers), 1)

    def test_parse_health_service_not_test_datacenter_with_self_enabled_filter(self):
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
                    'Weights': {
                        'Passing': 100,
                        'Warning': 0
                    }
                }
            }
        ]

        dc, servers = parse_consul_health_servers_data(value)

        self.assertEqual(len(servers), 0)

    def test_parse_retry_policy(self):
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
                                        "idempotent":"true"
                                    },
                                    "599":{
                                        "idempotent":"false"
                                    }
                                },
                                "max_tries":"3"
                            }
                        }
                    }
                }
            }"""
        }

        config = parse_consul_upstream_config(value)

        upstream = Upstream('some_upstream', config, [])
        self.assertEqual(upstream.config_by_profile.get(Upstream.DEFAULT_PROFILE).retry_policy.statuses,
                         {503: True, 599: False})
