import socket

from http_client import AIOHttpClientWrapper, HttpClientFactory, options
from http_client.balancing import RequestBalancerBuilder, Server, Upstream, UpstreamConfig
from pytest_httpserver import HTTPServer


class TestBase:
    def bind_unused_port(self):
        res = socket.getaddrinfo('127.0.0.1', 0, socket.AF_INET, socket.SOCK_STREAM, 0, socket.AI_PASSIVE)[0]
        af, socktype, proto, canonname, sockaddr = res
        sock = socket.socket(af, socktype, proto)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setblocking(False)
        sock.bind(sockaddr)
        sock.listen(128)
        return sock, sock.getsockname()[1]


class BalancingClientMixin:
    def setup_method(self, working_server: HTTPServer):
        self.request_balancer_builder = RequestBalancerBuilder({})
        self.http_client_factory = HttpClientFactory('testapp', AIOHttpClientWrapper(), self.request_balancer_builder)
        self.balancing_client = self.http_client_factory.get_http_client()
        options.datacenter = 'test'

    def get_upstream_config(self):
        return {
            Upstream.DEFAULT_PROFILE: UpstreamConfig(max_tries=3, request_timeout=0.5),
            "one_try": UpstreamConfig(max_tries=1, request_timeout=0.5),
            "two_tries": UpstreamConfig(max_tries=2, request_timeout=0.5)
        }

    def register_ports_for_upstream(self, *ports):
        self.servers = [Server(f'127.0.0.1:{port}', dc='test') for port in ports]
        upstream = Upstream('test', self.get_upstream_config(),
                            self.servers)
        self.request_balancer_builder._upstreams[upstream.name] = upstream
