#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-sftp",
    version="1.3.0",
    description="Singer.io tap for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_sftp"],
    install_requires=[
        "singer-python==6.0.0",
        'paramiko==2.6.0',
        'backoff==2.2.1',
        'singer-encodings==0.1.1',
        'singer-encodings==0.1.3',
        'terminaltables==3.1.0',
    ],
    extras_require={
        'dev': [
            'ipdb',
            'pylint',
            'nose2'
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
