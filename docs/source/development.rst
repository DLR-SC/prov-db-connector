Development
===========

.. note::

    We assume a working `virtual enviroment <http://docs.python-guide.org/en/latest/dev/virtualenvs/>`_ to work with.

Contribute
----------

Please, fork your the code on github and develop your feature in a new branch split from develop branch.
Commit to the main project by sending a pull request onto the develop branch

* Issue Tracker: https://github.com/DLR-SC/prov-db-connector/issues
* Source Code: https://github.com/DLR-SC/prov-db-connector

Setup
-----

1. Clone source from `project site <https://github.com/DLR-SC/prov-db-connector>`_
2. Change into the new directory

.. code:: sh

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

First you must create a class that extend from BaseAdapter and implement all functions.

.. code:: python

    from provdbconnector.db_adapters.baseadapter import BaseAdapter, DbDocument, DbBundle, DbRecord, DbRelation
    from provdbconnector.exceptions.database import InvalidOptionsException, NotFoundException
    from provdbconnector.utils.serializer import encode_dict_values_to_primitive
    from uuid import uuid4
    import logging

    log = logging.getLogger(__name__)

    document_bundle_ids = dict()

    bundles = dict() #dict for alle bundles including record and relation information

    all_records = dict() # separate dict for records only (to get them by id)

    class SimpleInMemoryAdapter(BaseAdapter):

        def __init__(self, *args):
            super(SimpleInMemoryAdapter, self).__init__()
            pass


        def connect(self, authentication_info):
            pass

        # other functions from BaseAdapter....

2. - Create test suites
~~~~~~~~~~~~~~~~~~~~~~~
To test your adapter you should create two test suits:

 * :py:class:`~provdbconnector.tests.db_adapters.in_memory.test_simple_in_memory.SimpleInMemoryAdapterTest` : Unit test for the low level functions in your adapter, **start here to develop and fulfill this tests first**.
 * :py:class:`~provdbconnector.tests.db_adapters.in_memory.test_simple_in_memory.SimpleInMemoryAdapterProvApiTests` : Integration test for the adapter with the api.

 See this example tests for the :py:class:`~provdbconnector.db_adapters.in_memory.simple_in_memory.SimpleInMemoryAdapter`.

.. code:: python

    from provdbconnector.exceptions.database import InvalidOptionsException
    from provdbconnector.db_adapters.in_memory import SimpleInMemoryAdapter
    from provdbconnector.provapi import ProvApi
    from provdbconnector.tests import AdapterTestTemplate
    from provdbconnector.tests import ProvApiTestTemplate

    class SimpleInMemoryAdapterTest(AdapterTestTemplate):

        def setUp(self):
            self.instance = SimpleInMemoryAdapter() #create a instnace of your adapter
            self.instance.connect(None) #connect to your database with credentials

        #test your connect method, because every adapater is different you have to write your own test
        def test_connect_invalid_options(self):
            authInfo = {"invalid": "Invalid"}
            with self.assertRaises(InvalidOptionsException):
                self.instance.connect(authInfo)

        def tearDown(self):
            del self.instance


    class SimpleInMemoryAdapterProvApiTests(ProvApiTestTemplate):

         def setUp(self):
             self.provapi = ProvApi(api_id=1, adapter=SimpleInMemoryAdapter, auth_info=None)

         def tearDown(self):
             del self.provapi

3. - Implement your adapter logic
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The last step is to create your logic inside the adapter for example the create and get bundle functions:


.. code:: python

    def create_bundle(self, document_id, attributes, metadata):
        #save the bundle information and return id as string
        document_id = document_id
        bundle_id = str(uuid4())


        #transform the attributes and metadata to primitive data types
        attr = encode_dict_values_to_primitive(attributes)
        meta = encode_dict_values_to_primitive(metadata)



        self.bundles.update({bundle_id: DbBundle(list(), DbRecord(attr,meta))})

        #save the bundle id to the document_bundle id map
        doc = self.document_bundle_ids.get(document_id)
        doc.append(bundle_id)

        return bundle_id


    def get_bundle(self, bundle_id):
        bundle = self.bundles.get(bundle_id)
        if bundle is None:
            raise NotFoundException()

        records = list()
        for record_id in bundle.records:
            record =self.all_records.get(record_id)
            records.append(record)

        return DbBundle(records, bundle.bundle_record)

`Here <https://github.com/DLR-SC/prov-db-connector/blob/master/provdbconnector/db_adapters/in_memory/simple_in_memory.py>`_ you can access the full example adapter

