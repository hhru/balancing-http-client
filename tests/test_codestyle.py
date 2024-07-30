import os.path
from functools import partial

import pycodestyle

from tests import ROOT


class TestCodestyle:
    CHECKED_PATHS = ('http_client', 'tests')

    def test_codestyle(self):
        style_guide = pycodestyle.StyleGuide(
            show_pep8=False,
            show_source=True,
            max_line_length=120,
            ignore=['E731', 'W504']
        )
        result = style_guide.check_files(map(partial(os.path.join, ROOT), TestCodestyle.CHECKED_PATHS))
        assert result.total_errors == 0, 'Pycodestyle found code style errors or warnings'
