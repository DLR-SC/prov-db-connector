#!/usr/bin/env python
try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup, find_packages

tests_require = [
    'coverage',
    'coveralls'
]

docs_require = [
    'Sphinx>=1.3.5',
    'recommonmark>=0.4.0',
    'sphinx-rtd-theme>=0.1.9',
    'sphinxcontrib-napoleon>=0.4.4',
    'sphinxcontrib-httpdomain>=1.5.0',
]

setup(
    name='prov-db-connector',
    version='0.2',
    description='PROV Database Connector',
    keywords=[
        'provenance', 'graph', 'model', 'PROV', 'PROV-DM', 'PROV-JSON', 'JSON',
        'PROV-XML', 'PROV-N'
    ],
    author='DLR, Stefan Bieliauskas, Martin Stoffers',
    author_email='opensource@dlr.de, sb@conts.de, martin.stoffers@studserv.uni-leipzig.de',
    url='https://github.com/DLR-SC/prov-db-connector',
    classifiers=[
        'Development Status :: 1 - Planning',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5'
    ],
    license="Apache License 2.0",

    packages=find_packages(),
    package_dir={
        'provdbconnector': 'provdbconnector'
    },
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        "prov==1.5.0",
        "neo4j-driver==1.0.2"
    ],
    extras_require={
        'test': tests_require,
        'dev': tests_require + docs_require,
        'docs': docs_require,
    },

    test_suite='provdbconnector.tests',
)
