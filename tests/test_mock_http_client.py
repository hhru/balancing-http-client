from collections.abc import AsyncGenerator

from http_client.testing import MockHttpClient
from tests.test_balancing_base import BalancingClientMixin, TestBase


class TestMockHttpClient(TestBase, BalancingClientMixin):
    async def test_simple_mock(self):
        with MockHttpClient() as mock_http_client:
            mock_http_client.add('http://mock_host/content', body='simple')

            response = await self.balancing_client.get_url('mock_host', '/content')
            assert response.status_code == 200
            assert response.raw_body == b'simple'

    async def test_streaming_mock(self) -> None:
        async def streaming_data(data: list[str]) -> AsyncGenerator[bytes, None]:
            for item in data:
                yield item.encode()

        animals = ['cat', 'dog', 'tiger']

        with MockHttpClient() as mock_http_client:
            mock_http_client.add(
                'http://mock_host/content', body=streaming_data(animals), headers={'Content-Type': 'text/event-stream'}
            )

            response = await self.balancing_client.get_url('mock_host', '/content')
            assert response.status_code == 200

            assert (await response.streaming_content.read()) == ''.join(animals).encode()
