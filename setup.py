#!/usr/bin/env python

from setuptools import setup

with open('README.md') as file:
    long_description = file.read()

setup(
    name='mongo2es',
    version='0.1',
    description='mongo2es',
    long_description=long_description,
    packages=['api'],
    include_package_data=True,
    install_requires=[
        'pymongo',
        'elasticsearch',
        'kafka-python',
        'raven[flask]',
        'python-etcd',
        'pyjwt'
    ],
)
