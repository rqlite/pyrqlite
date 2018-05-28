#!/usr/bin/env python

import os
from os.path import isdir, islink, relpath, dirname
import subprocess
import sys
from setuptools import (
    Command,
    setup,
    find_packages,
)

sys.path.insert(0, 'src')
from pyrqlite.constants import (
    __author__,
    __email__,
    __license__,
    __version__,
)

class PyTest(Command):
    user_options = [('match=', 'k', 'Run only tests that match the provided expressions')]

    def initialize_options(self):
        self.match = None

    def finalize_options(self):
        pass

    def run(self):
        testpath = 'src/test'
        buildlink = 'build/lib/test'

        if isdir(dirname(buildlink)):
            if islink(buildlink):
                os.unlink(buildlink)

            os.symlink(relpath(testpath, dirname(buildlink)), buildlink)
            testpath = buildlink

        try:
            os.environ['EPYTHON'] = 'python{}.{}'.format(sys.version_info.major, sys.version_info.minor)
            subprocess.check_call(['py.test', '-v', testpath, '-s',
                        '--cov-report=html', '--cov-report=term-missing'] +
                       (['-k', self.match] if self.match else []) +
                       ['--cov={}'.format(p) for p in find_packages(dirname(testpath), exclude=['test'])])

        finally:
            if islink(buildlink):
                os.unlink(buildlink)


class PyLint(Command):
    user_options = [('errorsonly', 'E', 'Check only errors with pylint'),
                    ('format=', 'f', 'Change the output format')]

    def initialize_options(self):
        self.errorsonly = 0
        self.format = 'colorized'

    def finalize_options(self):
        pass

    def run(self):
        cli_options = ['-E'] if self.errorsonly else []
        cli_options.append('--output-format={0}'.format(self.format))
        errno = subprocess.call(['pylint'] + cli_options + [
            "--msg-template='{C}:{msg_id}:{path}:{line:3d},{column}: {obj}: {msg} ({symbol})'"] +
            find_packages('src', exclude=['test']), cwd='./src')
        raise SystemExit(errno)


setup(
    name="pyrqlite",
    version=__version__,
    url='https://github.com/rqlite/pyrqlite/',
    author=__author__,
    author_email=__email__,
    maintainer=__author__,
    maintainer_email=__email__,
    description='python DB API 2.0 driver for rqlite',
    license=__license__,
    package_dir={'': 'src'},
    packages=find_packages('src', exclude=['test']),
    platforms=['Posix'],
    cmdclass={'test': PyTest, 'lint': PyLint},
    tests_require=['pytest', 'pytest-cov'],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy',
        'Topic :: Database',
    ],
)
