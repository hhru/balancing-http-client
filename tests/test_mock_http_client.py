from http_client.testing import MockHttpClient
from tests.test_balancing_base import BalancingClientMixin, TestBase


class TestMockHttpClient(TestBase, BalancingClientMixin):
    async def test_simple_mock(self):
        with MockHttpClient() as mock_http_client:
            mock_http_client.add('http://mock_host/content', body='simple')

            response = await self.balancing_client.get_url('mock_host', '/content')
            assert response.status_code == 200
            assert response.raw_body == b'simple'
