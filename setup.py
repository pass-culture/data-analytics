#!/usr/bin/env python
# -*- coding: utf-8 -*-
from setuptools import setup
from setuptools import find_packages

setup(
    name="pass-culture-data-analytics",
    version="1.0.0",
    packages=find_packages(exclude=["*_tests"]),
    license='Mozilla Public License 2.0',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    install_requires=[
        'python-dotenv==0.13.0',
        'Flask-SQLAlchemy==2.4.1',
        'freezegun==0.3.12',
        'pandas==1.1.2',
        'PostgreSQL-Audit==0.10.0',
        'psycopg2==2.7.7',
        'pytest==5.2.2',
        'SQLAlchemy==1.3.11',
        'psycopg2-binary==2.8.5',
        'freezegun==0.3.12',
        'requests==2.23.0',
        "black==20.8b1",
    ],
    python_requires="~=3.7",
    classifiers=[
        "Programming Language :: Python :: 3.7",
        "Operating System :: OS Independent",
        "Environment :: Console",
    ],
)
