import logging

from http_client.balancing import Server, UpstreamConfig
from http_client.model.consul_config import ConsulConfig
from http_client.options import options
from http_client.util import restore_original_datacenter_name

consul_util_logger = logging.getLogger('consul_parser')


def parse_consul_health_servers_data(values):
    service_config = {}
    servers = []
    dc = ''
    for v in values:
        node_name = v['Node']['Node'].lower()
        if len(v['Service']['Address']):
            service_config['Address'] = f'{v["Service"]["Address"]}:{v["Service"]["Port"]!s}'
        else:
            service_config['Address'] = f'{v["Node"]["Address"]}:{v["Service"]["Port"]!s}'
        service_config['Weight'] = v['Service']['Weights']['Passing']
        service_config['Datacenter'] = v['Node']['Datacenter']

        dc = restore_original_datacenter_name(service_config['Datacenter'])
        if options.self_node_filter_enabled and _not_same_name(node_name):
            consul_util_logger.debug(f'Self node filtering activated. Skip: {node_name}')
            continue
        servers.append(
            Server(address=service_config['Address'], hostname=node_name, weight=service_config['Weight'], dc=dc)
        )
        service_config = {}
    return dc, servers


def _not_same_name(node_name: str):
    return len(node_name) and options.node_name.lower() != node_name


def parse_consul_upstream_config(consul_data: dict[str, str]) -> dict[str, UpstreamConfig]:
    upstream_config = {}
    config: ConsulConfig = ConsulConfig.model_validate_json(consul_data['Value'])

    for profile_name, profile_config in config.hosts['default'].profiles.items():
        upstream_config[profile_name] = UpstreamConfig(
            max_tries=profile_config.max_tries,
            max_timeout_tries=profile_config.max_timeout_tries,
            connect_timeout=profile_config.connect_timeout_sec,
            request_timeout=profile_config.request_timeout_sec,
            speculative_timeout_pct=profile_config.speculative_timeout_pct,
            slow_start_interval=profile_config.slow_start_interval_sec,
            retry_policy=profile_config.retry_policy,
            session_required=profile_config.session_required,
        )
    return upstream_config
