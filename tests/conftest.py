import pytest
from pytest_httpserver import HTTPServer


@pytest.fixture(scope='function', autouse=True)
def working_server(httpserver: HTTPServer):
    httpserver.expect_request('/').respond_with_json(None, status=200)
    return httpserver
