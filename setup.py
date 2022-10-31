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
    'celery<5',
    'requests',
    'datapackage>=1.14.0',
    'tableschema>=1.2.5',
    'tableschema-sql>=0.10.4',
    'pyyaml',
    'ujson',
    'mistune<2',
    'markupsafe==2.0.1',
    'redis>=3,<4',
    'click<8.0',
    'awesome-slugify',
    'flask<2.0.0',
    'flask-cors',
    'flask-jsonpify',
    'flask-basicauth',
    'cachetools',
    'tabulator>=1.50.0',
    'globster>=0.1.0',
    'dataflows>=0.2.11',
    'python-dateutil<2.8.1',
    'werkzeug<1.0'
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
    long_description_content_type='text/markdown',
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
