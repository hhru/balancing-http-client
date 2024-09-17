from tests.test_balancing_base import BalancingClientMixin, TestBase
from http_client.testing import MockHttpClient


class TestMockHttpClient(TestBase, BalancingClientMixin):
    async def test_simple_mock(self):
        with MockHttpClient() as mock_http_client:
            mock_http_client.add('http://test/content', body='simple')

            response = await self.balancing_client.get_url('test', '/content')
            assert response.status_code == 200
            assert response.raw_body == b'simple'
