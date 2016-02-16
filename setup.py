# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

import os
import io
from setuptools import setup, find_packages

BASEDIR = os.path.dirname(__file__)


def read(f, method='read'):
    """Read a text file at the given relative path."""
    filepath = os.path.join(BASEDIR, f)
    return getattr(io.open(filepath, encoding='utf-8'), method)().strip()

PACKAGE = 'oki'
VERSION = read(os.path.join(BASEDIR, PACKAGE, 'VERSION'), 'readline')
README = read('README.md')
LICENSE = read('LICENSE.md')
INSTALL_REQUIRES = ['six>=1.9']
TESTS_REQUIRE = ['tox']

setup(
    name=PACKAGE,
    version=VERSION,
    description='We want the data raw, and we want the data now.',
    long_description=README,
    author='Open Knowledge International',
    author_email='info@okfn.org',
    url='https://github.com/okfn/opendata-py',
    license='MIT',
    include_package_data=True,
    packages=find_packages(exclude=['tests']),
    package_dir={PACKAGE: PACKAGE},
    install_requires=INSTALL_REQUIRES,
    tests_require=TESTS_REQUIRE,
    test_suite='make test',
    zip_safe=False,
    keywords='raw data',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ]
)
