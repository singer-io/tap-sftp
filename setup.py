#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-sftp",
    version="1.2.1",
    description="Singer.io tap for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_sftp"],
    install_requires=[
        "singer-python==5.12.1",
        'paramiko==2.6.0',
        'backoff==1.8.0',
        'singer-encodings==0.1.3',
    ],
    extras_require={
        'dev': [
            'ipdb',
            'pylint',
            'nose'
        ],
        'test': [
            'paramiko==2.6.0'
        ]
    },
    entry_points="""
    [console_scripts]
    tap-sftp=tap_sftp:main
    """,
    packages=["tap_sftp"],
    package_data = {
        "schemas": ["tap_sftp/schemas/*.json"]
    },
    include_package_data=True,
)
