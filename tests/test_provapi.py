import os
import unittest
from uuid import UUID
import tests.examples as examples
from provdbconnector import ProvApi
from provdbconnector.databases import InvalidOptionsException
from provdbconnector.databases import Neo4jAdapter
from provdbconnector.provapi import NoDataBaseAdapterException,InvalidArgumentTypeException

class ProvApiTestTemplate(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        """
        Prevent from execute the test case directly see:

        http://stackoverflow.com/questions/4566910/abstract-test-case-using-python-unittest

        :param args:
        :param kwargs:
        """
        super(ProvApiTestTemplate, self).__init__(*args, **kwargs)
        self.helper = None
        # Kludge alert: We want this class to carry test cases without being run
        # by the unit test framework, so the `run' method is overridden to do
        # nothing.  But in order for sub-classes to be able to do something when
        # run is invoked, the constructor will rebind `run' from TestCase.
        if self.__class__ != ProvApiTestTemplate:
            # Rebind `run' from the parent class.
            self.run = unittest.TestCase.run.__get__(self, self.__class__)
        else:
            self.run = lambda self, *args, **kwargs: None


    def test_prov_primer_example(self):
        prov_document = examples.primer_example()
        stored_document_id = self.instance.create_document_from_prov(prov_document)
        stored_document = self.instance.get_document_as_prov(prov_document)

        self.assertNotEqual(stored_document, prov_document)

    def primer_example_alternate(self):
        prov_document = examples.primer_example_alternate()
        stored_document_id = self.instance.create_document_from_prov(prov_document)
        stored_document = self.instance.get_document_as_prov(prov_document)

        self.assertNotEqual(stored_document, prov_document)

    def w3c_publication_1(self):
        prov_document = examples.w3c_publication_1()
        stored_document_id = self.instance.create_document_from_prov(prov_document)
        stored_document = self.instance.get_document_as_prov(prov_document)

        self.assertNotEqual(stored_document, prov_document)

    def w3c_publication_2(self):
        prov_document = examples.w3c_publication_2()
        stored_document_id = self.instance.create_document_from_prov(prov_document)
        stored_document = self.instance.get_document_as_prov(prov_document)

        self.assertNotEqual(stored_document, prov_document)

    def bundles1(self):
        prov_document = examples.bundles1()
        stored_document_id = self.instance.create_document_from_prov(prov_document)
        stored_document = self.instance.get_document_as_prov(prov_document)

        self.assertNotEqual(stored_document, prov_document)

    def bundles2(self):
        prov_document = examples.bundles2()
        stored_document_id = self.instance.create_document_from_prov(prov_document)
        stored_document = self.instance.get_document_as_prov(prov_document)

        self.assertNotEqual(stored_document, prov_document)

    def collections(self):
        prov_document = examples.collections()
        stored_document_id = self.instance.create_document_from_prov(prov_document)
        stored_document = self.instance.get_document_as_prov(prov_document)

        self.assertNotEqual(stored_document, prov_document)

    def long_literals(self):
        prov_document = examples.long_literals()
        stored_document_id = self.instance.create_document_from_prov(prov_document)
        stored_document = self.instance.get_document_as_prov(prov_document)

        self.assertNotEqual(stored_document, prov_document)

    def datatypes(self):
        prov_document = examples.datatypes()
        stored_document_id = self.instance.create_document_from_prov(prov_document)
        stored_document = self.instance.get_document_as_prov(prov_document)

        self.assertNotEqual(stored_document, prov_document)



class ProvApiTests(unittest.TestCase):

    def setUp(self):
        self.authInfo = {"user_name": os.environ.get('NEO4J_USERNAME', 'neo4j'),
                         "user_password": os.environ.get('NEO4J_PASSWORD', 'neo4jneo4j'),
                         "host": os.environ.get('NEO4J_HOST', 'localhost:7687')
        }
        self.provapi = ProvApi(id=1, adapter=Neo4jAdapter, authinfo=self.authInfo)

    def tearDown(self):
        pass

    #Test create instnace
    def test_provapi_instance(self):
        self.assertRaises(NoDataBaseAdapterException, lambda: ProvApi())
        self.assertRaises(InvalidOptionsException, lambda: ProvApi(id=1, adapter=Neo4jAdapter))

        obj = ProvApi(id=1, adapter=Neo4jAdapter, authinfo=self.authInfo)
        self.assertIsInstance(obj, ProvApi)
        self.assertEquals(obj.apiid, 1)

        obj = ProvApi(adapter=Neo4jAdapter, authinfo=self.authInfo)
        self.assertIsInstance(obj.apiid,UUID)

    #Methods that automatically convert to ProvDocument
    def test_create_document_from_json(self):
        self.provapi.create_document_from_json()

    def test_get_document_as_json(self):
        self.provapi.get_document_as_json()

    def test_create_document_from_xml(self):
        raise NotImplementedError()

    def test_get_document_as_xml(self):
        raise NotImplementedError()

    def test_create_document_from_provn(self):
        raise NotImplementedError()

    def test_get_document_as_provn(self):
        raise NotImplementedError()

    #Methods with ProvDocument input / output
    def test_create_document_from_prov(self):
        example = examples.primer_example()
        document_id = self.provapi.create_document_from_prov(example)
        self.assertIsNone(document_id)
        self.assertIsInstance(document_id, str)

    def test_create_document_from_prov_invalid_arguments(self):

        with self.assertRaises(InvalidArgumentTypeException):
            self.provapi.create_document_from_prov(None)


    def test_get_document_as_prov(self):
        self.provapi.get_document_as_prov()

    def test_create_bundle_invalid_arguments(self):

        with self.assertRaises(InvalidArgumentTypeException):
            self.provapi._create_bundle("xxxx",None)
