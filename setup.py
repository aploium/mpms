#!/usr/bin/env python3
# coding=utf-8
from __future__ import unicode_literals
from setuptools import setup, find_packages

import mpms

PACKAGE = "mpms"
NAME = "mpms"
DESCRIPTION = "Simple python Multiprocesses-Multithreads task queue"
AUTHOR = "aploium"
AUTHOR_EMAIL = "i@z.codes"
URL = "https://github.com/aploium/mpms"

setup(
    name=NAME,
    version=mpms.VERSION_STR,
    description=DESCRIPTION,
    author=AUTHOR,
    author_email=AUTHOR_EMAIL,
    url=URL,
    packages=find_packages(),
    py_modules=['mpms'],
    platforms="any",
    zip_safe=False,
    classifiers=[
        'Development Status :: 4 - Beta',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        # 'Programming Language :: Python :: 2.7',
        # 'Programming Language :: Python :: 3.4',
        # 'Programming Language :: Python :: 3.5',
        # 'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
    ]
)
