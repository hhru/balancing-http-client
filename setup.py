import sys

from setuptools import setup
from setuptools.command.test import test


class TestHook(test):
    user_options = [('with-coverage', 'c', 'Run test suite with coverage')]

    def initialize_options(self):
        self.with_coverage = False
        test.initialize_options(self)

    def run_tests(self):
        import pytest
        sys.exit(pytest.main(['tests', '--tb', 'native']))


setup(
    name='balancing-http-client',
    version='1.3.4',
    description='Balancing http client for tornado',
    url='https://github.com/hhru/balancing-http-client',
    cmdclass={
        'test': TestHook
    },
    packages=[
        'http_client'
    ],
    python_requires='>=3.6',
    install_requires=[
        'tornado >= 5.1',
        'pycurl >= 7.43.0',
        'lxml >= 3.5.0',
    ],
    test_suite='tests',
    tests_require=[
        'pytest <= 3.8.2',
        'pycodestyle == 2.5.0',
        'tornado >= 5.1',
    ],
    zip_safe=False
)
