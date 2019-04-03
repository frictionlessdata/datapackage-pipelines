# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

import os
import io
from setuptools import setup, find_packages


# Helpers
def read(*paths):
    """Read a text file."""
    basedir = os.path.dirname(__file__)
    fullpath = os.path.join(basedir, *paths)
    contents = io.open(fullpath, encoding='utf-8').read().strip()
    return contents


# Prepare
PACKAGE = 'datapackage_pipelines'
NAME = PACKAGE.replace('_', '-')
INSTALL_REQUIRES = [
    'celery',
    'requests',
    'datapackage>=1.5.1',
    'tableschema>=1.2.5',
    'tableschema-sql>=0.10.4',
    'pyyaml',
    'ujson',
    'mistune',
    'redis>=3,<4',
    'click<8.0',
    'awesome-slugify',
    'flask<2.0.0',
    'flask-cors',
    'flask-jsonpify',
    'flask-basicauth',
    'cachetools',
    'tabulator>=1.17.0',
    'globster>=0.1.0',
    'dataflows>=0.0.34',
]
SPEEDUP_REQUIRES = [
    'dataflows[speedup]',
]
LINT_REQUIRES = [
    'pylama',
]
TESTS_REQUIRE = [
    'tox',
    'sqlalchemy',
]
README = read('README.md')
VERSION = read(PACKAGE, 'VERSION')
PACKAGES = find_packages(exclude=['examples', 'tests', '.tox'])

# Run
setup(
    name=NAME,
    version=VERSION,
    packages=PACKAGES,
    include_package_data=True,
    install_requires=INSTALL_REQUIRES,
    tests_require=TESTS_REQUIRE,
    extras_require={
        'develop': LINT_REQUIRES + TESTS_REQUIRE,
        'speedup': SPEEDUP_REQUIRES,
    },
    zip_safe=False,
    long_description=README,
    description='{{ DESCRIPTION }}',
    author='Open Knowledge Foundation',
    author_email='info@okfn.org',
    url='https://github.com/frictionlessdata/datapackage-pipelines',
    license='MIT',
    keywords=[
        'data',
    ],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3.6',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    entry_points={
      'console_scripts': [
        'dpp = datapackage_pipelines.cli:cli',
      ]
    },
)
