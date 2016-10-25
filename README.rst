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

This python module provides a general interface to save `W3C-Prov <https://www.w3.org/TR/prov-overview/>`_-Documents into a database.
Currently we support the `Neo4j <https://neo4j.com/>`_-Graph database.

Read the full documentation at: `prov-db-connector.readthedocs.io <http://prov-db-connector.readthedocs.io/en/latest/>`_

We transform a prov document into a graph structure and the result looks like this:

.. figure:: _images/complex_example_with_neo4j_graph.png
   :align: center
   :scale: 50 %
   :alt: Complex example in Neo4j

   Complex example in Neo4j

Installation
------------

.. note::

    We assume a working `virtual enviroment <http://docs.python-guide.org/en/latest/dev/virtualenvs/>`_ to work with.

PyPi
~~~~

.. warning::

    Not published, yet.

Install it by running::

    pip install prov-db-connector

You can view `prov-db-connector on PyPi's package index <https://pypi.python.org/pypi/prov-db-connector/>`_

Source
~~~~~~

1. Clone source from `project site <https://github.com/DLR-SC/prov-db-connector>`_
2. Change into the new directory

.. code:: sh

    make setup

Usage
-----

Example: Save/Get prov document
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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


