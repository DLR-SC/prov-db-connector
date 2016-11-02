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

.. figure:: _images/complex_example_with_neo4j_graph.png

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

.. code:: python

    from prov.model import ProvDocument
    from provdbconnector import ProvApi
    from provdbconnector.db_adapters.in_memory import SimpleInMemoryAdapter

    prov_api = ProvApi(adapter=SimpleInMemoryAdapter, auth_info=None)

    # create the prov document
    prov_document = ProvDocument()
    prov_document.add_namespace("ex", "http://example.com")

    prov_document.agent("ex:Bob")
    prov_document.activity("ex:Alice")

    prov_document.association("ex:Alice", "ex:Bob")

    document_id = prov_api.create_document(prov_document)

    print(prov_api.get_document_as_provn(document_id))

    # Output:
    #
    # document
    # prefix
    # ex < http: // example.com >
    #
    # agent(ex:Bob)
    # activity(ex:Alice, -, -)
    # wasAssociatedWith(ex:Alice, ex:Bob, -)
    # endDocument

File Buffer example
~~~~~~~~~~~~~~~~~~~

.. code:: python

    from provdbconnector import ProvApi
    from provdbconnector.db_adapters.in_memory import SimpleInMemoryAdapter
    import pkg_resources

    # create the api
    prov_api = ProvApi(adapter=SimpleInMemoryAdapter, auth_info=None)

    # create the prov document from examples
    prov_document_buffer = pkg_resources.resource_stream("examples", "file_buffer_example_primer.json")

    # Save document
    document_id = prov_api.create_document(prov_document_buffer)
    # This is similar to:
    # prov_api.create_document_from_json(prov_document_buffer)

    # get document
    print(prov_api.get_document_as_provn(document_id))

    # Output:

    ...

You find all examples in the `examples <https://github.com/DLR-SC/prov-db-connector/tree/master/examples>`_ folder

License
-------

See `LICENSE <https://github.com/DLR-SC/prov-db-connector/blob/master/LICENSE>`_ file


