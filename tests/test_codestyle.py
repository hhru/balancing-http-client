from pystolint.api import check

from tests import ROOT

MODULES = ['http_client', 'tests']


def test_codestyle() -> None:
    result = check(MODULES, diff=True, local_toml_path_provided=f'{ROOT}/pyproject.toml')

    assert len(result.items) == 0, '\n'.join(str(item) for item in result.items)
    assert len(result.errors) == 0, '\n'.join(error for error in result.errors)
