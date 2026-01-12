#!/usr/bin/env python3
# coding=utf-8
from __future__ import unicode_literals

from pathlib import Path
import re

from setuptools import setup, find_packages

PACKAGE = "mpms"
NAME = "mpms"
DESCRIPTION = "Simple python Multiprocesses-Multithreads task queue"
AUTHOR = "aploium"
AUTHOR_EMAIL = "i@z.codes"
URL = "https://github.com/aploium/mpms"

_here = Path(__file__).resolve().parent
_readme_path = _here / "README.md"
if not _readme_path.exists():
    _readme_path = _here / "readme.md"
_long_description = _readme_path.read_text(encoding="utf-8")


def _read_version() -> str:
    mpms_py = (_here / "mpms.py").read_text(encoding="utf-8")
    match = re.search(r"^VERSION\s*=\s*\(([^)]*)\)", mpms_py, flags=re.MULTILINE)
    if not match:
        raise RuntimeError("Cannot find VERSION tuple in mpms.py")

    raw_parts = [p.strip() for p in match.group(1).split(",")]
    parts = [int(p) for p in raw_parts if p]
    return ".".join(str(p) for p in parts)

setup(
    name=NAME,
    version=_read_version(),
    description=DESCRIPTION,
    long_description=_long_description,
    long_description_content_type="text/markdown",
    author=AUTHOR,
    author_email=AUTHOR_EMAIL,
    url=URL,
    project_urls={
        "Source": URL,
        "Changelog": f"{URL}/blob/master/CHANGELOG.md",
    },
    packages=find_packages(exclude=("tests", "tests.*")),
    py_modules=['mpms'],
    python_requires=">=3.10",
    platforms="any",
    zip_safe=False,
    classifiers=[
        'Development Status :: 4 - Beta',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
        'Programming Language :: Python :: 3.13',
    ]
)
