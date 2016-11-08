PROV Database Connector
=======================

Introduction
------------

.. image:: https://travis-ci.org/DLR-SC/prov-db-connector.svg?branch=master
  :target: https://travis-ci.org/DLR-SC/prov-db-connector
  :alt: Build Status
.. image:: https://coveralls.io/repos/github/DLR-SC/prov-db-connector/badge.svg?branch=master
  :target: https://coveralls.io/github/DLR-SC/prov-db-connector?branch=master
  :alt: Coverage Status
.. image:: https://www.quantifiedcode.com/api/v1/project/3ee099c99b0340728ca4d54392caae83/badge.svg
  :target: https://www.quantifiedcode.com/app/project/3ee099c99b0340728ca4d54392caae83
  :alt: Code Issues

This python module provides a general interface to save `W3C-PROV <https://www.w3.org/TR/prov-overview/>`_ documents into databases.
Currently we support the `Neo4j <https://neo4j.com/>`_ graph database.

We transform a PROV document into a graph structure and the result looks like this:

.. figure:: https://cdn.rawgit.com/dlr-sc/prov-db-connector/develop/docs/source/_images/test_cases/test_prov_primer_example.svg
   :align: center
   :scale: 50 %
   :alt: Complex example in Neo4j

   Complex example in Neo4j

See full documentation at: `prov-db-connector.readthedocs.io <http://prov-db-connector.readthedocs.io>`_

Installation
------------

PyPi
~~~~

.. warning::

    Not published, yet.

Install it by running::

    pip install prov-db-connector

You can view `prov-db-connector on PyPi's package index <https://pypi.python.org/pypi/prov-db-connector/>`_

Source
~~~~~~

.. code:: sh

    # Clone project
    git clone git@github.com:DLR-SC/prov-db-connector.git
    cd prov-db-connector

    # Setup virtual environment
    virtualenv -p /usr/bin/python3.4 env
    source env/bin/activate

    # Install dependencies and package into virtual enviroment
    make setup

Usage
-----

Save and get prov document example
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. literalinclude:: /examples/simple_example.py
   :language: python

File Buffer example
~~~~~~~~~~~~~~~~~~~

.. literalinclude:: /examples/file_buffer_example.py
   :language: python

You find all examples in the `examples <https://github.com/DLR-SC/prov-db-connector/tree/master/examples>`_ folder

License
-------

See `LICENSE <https://github.com/DLR-SC/prov-db-connector/blob/master/LICENSE>`_ file


