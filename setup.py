#!/usr/bin/env python

from setuptools import setup, find_packages
import versioneer


requires = open('requirements.txt').read().strip().split('\n')

setup(
    name='intake-elasticsearch',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description='ElasticSearch plugin for Intake',
    url='https://github.com/ContinuumIO/intake-elasticsearch',
    maintainer='Stan Seibert',
    maintainer_email='sseibert@anaconda.com',
    license='BSD',
    py_modules=['intake_elasticsearch'],
    packages=find_packages(),
    entry_points={
        'intake.drivers': [
            'elasticsearch_seq = intake_elasticsearch.elasticsearch_seq:ElasticSearchSeqSource',
            'elasticsearch_table = intake_elasticsearch.elasticsearch_table:ElasticSearchTableSource',
        ]},
    package_data={'': ['*.csv', '*.yml', '*.html']},
    include_package_data=True,
    install_requires=requires,
    long_description=open('README.rst').read(),
    zip_safe=False,
)
