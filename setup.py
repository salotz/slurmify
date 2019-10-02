#!/usr/bin/env python
# -*- encoding: utf-8 -*-
from __future__ import absolute_import
from __future__ import print_function

import re
from glob import glob
from os.path import basename
from os.path import dirname
from os.path import join
from os.path import splitext

from setuptools import setup, find_packages

setup(
    # package metadata
    name="slurmify",
    version="0.1-a",
    author="Samuel D. Lotz",
    author_email="samuel.lotz@salotz.info",
    description=("Submit jobs to slurm with python."),
    license="MIT",
    keywords="cluster slurm scheduler",
    url="https://github.com/salotz/slurmify",
    long_description=open('README.org').read(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Topic :: Utilities",
        "License :: OSI Approved :: MIT License",
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 3'
    ],
    # building/dev
    setup_requires=['pytest-runner'],
    tests_require=['pytest', 'tox'],
    # package
    packages=find_packages('src'),
    package_dir={'' : 'src'},
    # if this is true then the package_data won't be included in the
    # dist, and I prefer this to MANIFEST
    include_package_data=False,
    package_data={'slurmify' : ['templates/*.j2']},
    entry_points={
        'console_scripts': ['slurmify = slurmify.cli:cli']
        },
    install_requires=[
        'jinja2',
        'click',
        'toml',
        'colorama'
    ],
)
