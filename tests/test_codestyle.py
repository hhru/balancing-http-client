import os.path
from functools import partial

import pycodestyle
import mypy.api
import sys

from tests import ROOT


class TestCodestyle:
    CHECKED_PATHS = ('http_client', 'tests')

    def test_pycodestyle(self):
        style_guide = pycodestyle.StyleGuide(
            show_pep8=False,
            show_source=True,
            max_line_length=120,
            ignore=['E731', 'W504']
        )
        result = style_guide.check_files(map(partial(os.path.join, ROOT), TestCodestyle.CHECKED_PATHS))
        assert result.total_errors == 0, 'Pycodestyle found code style errors or warnings'

    # TODO fix all type issues
    def test_mypy(self):
        pass
        # code_paths = [f'{ROOT}/http_client', f'{ROOT}/tests']
        # out, err, exit_code = mypy.api.run(['--config-file', f'{ROOT}/pyproject.toml'] + code_paths)
        # sys.stdout.write(out)
        # sys.stderr.write(err)
        # assert exit_code == 0, out
