Development
===========

Contribute
----------

Please, fork the code on Github and develop your feature in a new branch split from the develop branch.
Commit your code to the main project by sending a pull request onto the develop branch

* Issue Tracker: https://github.com/DLR-SC/prov-db-connector/issues
* Source Code: https://github.com/DLR-SC/prov-db-connector

Setup
-----

.. code:: sh

    # Clone project
    git clone git@github.com:DLR-SC/prov-db-connector.git
    cd prov-db-connector

    # Setup virtual environment
    virtualenv -p /usr/bin/python3.4 env
    source env/bin/activate

    # Install dependencies
    make dev-setup

Execute tests
-------------

.. code:: sh

    make test

Coverage report
---------------

.. code:: sh

    make coverage

Compile documentation
---------------------

.. code:: sh

    make docs

Create new database adapters
----------------------------

The database adapters are the binding class to the actual database.
If you are consider to build your own adapter please keep in mind:

* All adapters **must** enhance the :py:class:`~provdbconnector.db_adapters.baseadapter.Baseadapter` class.
* You **must** implement all specified functions in BaseAdapter
* You **should** test it via the :py:class:`~provdbconnector.tests.db_adapters.test_baseadapter.AdapterTestTemplate` class template.
* You **should** test it also via the :py:class:`~provdbconnector.tests.test_provapi.ProvApiTestTemplate` class template.

1. - Create your database adapter
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

First you must create a class that extend from :py:class:`~provdbconnector.db_adapters.baseadapter.Baseadapter` and implement all functions.

.. literalinclude:: ../../provdbconnector/db_adapters/in_memory/simple_in_memory.py
   :linenos:
   :lines: 1-30
   :language: python
   :emphasize-lines: 26-30

2. - Create test suites
~~~~~~~~~~~~~~~~~~~~~~~

To test your adapter you should create two test suits:

 * :py:class:`~provdbconnector.tests.db_adapters.in_memory.test_simple_in_memory.SimpleInMemoryAdapterTest` : Unit test for the low level functions in your adapter.
 * For further introduction on testing your database adapter have a look at the :ref:`test_howto`.
 * :py:class:`~provdbconnector.tests.db_adapters.in_memory.test_simple_in_memory.SimpleInMemoryAdapterProvApiTests` : Integration test for the adapter with the api.

See this example tests for the :py:class:`~provdbconnector.db_adapters.in_memory.simple_in_memory.SimpleInMemoryAdapter`

.. literalinclude:: ../../provdbconnector/tests/db_adapters/in_memory/test_simple_in_memory.py
   :linenos:
   :language: python


3. - Implement your adapter logic
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The last step is to create your logic inside the :py:class:`~provdbconnector.db_adapters.in_memory.simple_in_memory.SimpleInMemoryAdapter` for example the save_record and get_record functions:

.. literalinclude:: ../../provdbconnector/db_adapters/in_memory/simple_in_memory.py
   :linenos:
   :lines: 32-56, 105-115
   :language: python
