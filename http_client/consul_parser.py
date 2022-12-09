import json
import logging

from http_client.util import restore_original_datacenter_name

from http_client.balancing import Server

from http_client.options import options


consul_util_logger = logging.getLogger('consul_parser')


def parse_consul_health_servers_data(values):
    service_config = {}
    servers = []
    dc = ''
    for v in values:
        node_name = v['Node']['Node'].lower()
        if len(v['Service']['Address']):
            service_config['Address'] = f"{v['Service']['Address']}:{str(v['Service']['Port'])}"
        else:
            service_config['Address'] = f"{v['Node']['Address']}:{str(v['Service']['Port'])}"
        service_config['Weight'] = v['Service']['Weights']['Passing']
        service_config['Datacenter'] = v['Node']['Datacenter']

        dc = restore_original_datacenter_name(service_config['Datacenter'])
        if options.self_node_filter_enabled and _not_same_name(node_name):
            consul_util_logger.debug(f'Self node filtering activated. Skip: {node_name}')
            continue
        servers.append(Server(
            address=service_config['Address'],
            weight=service_config['Weight'],
            dc=dc
        ))
        service_config = {}
    return dc, servers


def _not_same_name(node_name: str):
    return len(node_name) and options.node_name.lower() != node_name


def parse_consul_upstream_config(value):
    config = {}
    values = json.loads(value['Value'])
    default_profile = values['hosts']['default']['profiles']['default']
    config['retry_policy'] = default_profile.get('retry_policy', {})
    config['request_timeout_sec'] = default_profile.get('request_timeout_sec',
                                                        options.http_client_default_request_timeout_sec)
    config['max_timeout_tries'] = default_profile.get('max_timeout_tries',
                                                      options.http_client_default_max_timeout_tries)
    config['max_tries'] = default_profile.get('max_tries', options.http_client_default_max_tries)
    config['connect_timeout_sec'] = default_profile.get('connect_timeout_sec',
                                                        options.http_client_default_connect_timeout_sec)

    config['slow_start_interval_sec'] = default_profile.get('slow_start_interval_sec', 0)
    config['speculative_timeout_pct'] = default_profile.get('speculative_timeout_pct', 0)
    config['session_required'] = default_profile.get('session_required', options.http_client_default_session_required)

    return config
