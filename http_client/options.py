from dataclasses import dataclass, field, fields


@dataclass
class Options:
    app: str = None
    datacenter: str = None
    datacenters: list = field(default_factory=lambda: [])

    timeout_multiplier: float = 1.0
    http_client_default_connect_timeout_sec: float = 0.2
    http_client_default_request_timeout_sec: float = 2.0
    http_client_default_max_tries: int = 2
    http_client_default_max_timeout_tries: int = 1
    http_client_default_retry_policy: dict = field(default_factory=lambda: {599: False, 503: False})
    http_client_default_retry_policy_cassandra: str = 'timeout,http_503'
    http_client_default_session_required: bool = False
    http_proxy_host: str = None
    http_proxy_port: int = 3128
    http_client_allow_cross_datacenter_requests: bool = False
    self_node_filter_enabled: bool = False
    node_name: str = ''
    max_clients: int = 100
    use_orjson: bool = True
    balancing_requests_log_level: str = 'debug'
    adaptive: bool = False
    log_adaptive_statistics: bool = False


options = Options()


def parse_config_file(path):
    config = {}
    with open(path, 'rb') as config_file:
        exec(config_file.read(), config, config)

    for f in fields(Options):
        setattr(options, f.name, config.get(f.name, getattr(options, f.name)))
