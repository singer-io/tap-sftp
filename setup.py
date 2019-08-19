#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-ftp",
    version="1.3.4",
    description="Singer.io tap for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_ftp"],
    install_requires=[
        "singer-python==5.5.1",
    ],
    extras_require={
        'dev': [
            'ipdb==0.11',
            'pylint==2.1.1',
        ]
    },
    entry_points="""
    [console_scripts]
    tap-ftp=tap_ftp:main
    """,
    packages=["tap_ftp"],
    package_data = {
        "schemas": ["tap_ftp/schemas/*.json"]
    },
    include_package_data=True,
)
