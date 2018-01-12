#!/usr/bin/env python

from setuptools import setup, find_packages
version = '0.0.1'


requires = open('requirements.txt').read().strip().split('\n')

setup(
    name='intake-elasticsearch',
    version=version,
    description='ElasticSearch plugin for Intake',
    url='https://github.com/ContinuumIO/intake-elasticsearch',
    maintainer='Stan Seibert',
    maintainer_email='sseibert@anaconda.com',
    license='BSD',
    py_modules=['intake_elasticsearch'],
    packages=find_packages(),
    package_data={'': ['*.csv', '*.yml', '*.html']},
    include_package_data=True,
    install_requires=requires,
    long_description=open('README.rst').read(),
    zip_safe=False,
)
