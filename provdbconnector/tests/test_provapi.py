import unittest
from uuid import UUID

import pkg_resources
from prov.model import ProvDocument

from provdbconnector.tests import examples as examples
from provdbconnector import ProvApi
from provdbconnector.exceptions.database import InvalidOptionsException
from provdbconnector.db_adapters.neo4j import Neo4jAdapter, NEO4J_USER, NEO4J_PASS, NEO4J_HOST, NEO4J_BOLT_PORT
from provdbconnector.db_adapters.baseadapter import METADATA_KEY_TYPE_MAP, METADATA_KEY_PROV_TYPE, \
    METADATA_KEY_IDENTIFIER, METADATA_KEY_NAMESPACES
from provdbconnector.exceptions.provapi import NoDataBaseAdapterException, InvalidArgumentTypeException


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

    def setUp(self):
        # this function will never be executed !!!!
        self.provapi = ProvApi()

    def test_prov_primer_example(self):
        prov_document = examples.primer_example()
        stored_document_id = self.provapi.create_document_from_prov(prov_document)
        stored_document = self.provapi.get_document_as_prov(stored_document_id)

        self.assertEqual(stored_document, prov_document)

    def test_primer_example_alternate(self):
        prov_document = examples.primer_example_alternate()
        stored_document_id = self.provapi.create_document_from_prov(prov_document)
        stored_document = self.provapi.get_document_as_prov(stored_document_id)

        self.assertEqual(stored_document, prov_document)

    def test_w3c_publication_1(self):
        prov_document = examples.w3c_publication_1()
        stored_document_id = self.provapi.create_document_from_prov(prov_document)
        stored_document = self.provapi.get_document_as_prov(stored_document_id)

        self.assertEqual(stored_document, prov_document)

    def test_w3c_publication_2(self):
        prov_document = examples.w3c_publication_2()
        stored_document_id = self.provapi.create_document_from_prov(prov_document)
        stored_document = self.provapi.get_document_as_prov(stored_document_id)

        self.assertEqual(stored_document, prov_document)

    def test_bundles1(self):
        prov_document = examples.bundles1()
        stored_document_id = self.provapi.create_document_from_prov(prov_document)
        stored_document = self.provapi.get_document_as_prov(stored_document_id)

        self.assertEqual(stored_document, prov_document)

    def test_bundles2(self):
        prov_document = examples.bundles2()
        stored_document_id = self.provapi.create_document_from_prov(prov_document)
        stored_document = self.provapi.get_document_as_prov(stored_document_id)

        self.assertEqual(stored_document, prov_document)

    def test_collections(self):
        prov_document = examples.collections()
        stored_document_id = self.provapi.create_document_from_prov(prov_document)
        stored_document = self.provapi.get_document_as_prov(stored_document_id)

        self.assertEqual(stored_document, prov_document)

    def test_long_literals(self):
        prov_document = examples.long_literals()
        stored_document_id = self.provapi.create_document_from_prov(prov_document)
        stored_document = self.provapi.get_document_as_prov(stored_document_id)

        self.assertEqual(stored_document, prov_document)

    def test_datatypes(self):
        prov_document = examples.datatypes()
        stored_document_id = self.provapi.create_document_from_prov(prov_document)
        stored_document = self.provapi.get_document_as_prov(stored_document_id)

        self.assertEqual(stored_document, prov_document)


class ProvApiTests(unittest.TestCase):
    maxDiff = None

    def setUp(self):
        self.test_resources = {
            'xml': {'package': 'provdbconnector', 'file': '/tests/resources/primer.provx'},
            'json': {'package': 'provdbconnector', 'file': '/tests/resources/primer.json'},
            'provn': {'package': 'provdbconnector', 'file': '/tests/resources/primer.provn'}
        }
        self.test_prov_files = dict((key, pkg_resources.resource_stream(val['package'], val['file'])) for key, val in
                                    self.test_resources.items())
        self.auth_info = {"user_name": NEO4J_USER,
                          "user_password": NEO4J_PASS,
                          "host": NEO4J_HOST + ":" + NEO4J_BOLT_PORT
                          }
        self.provapi = ProvApi(api_id=1, adapter=Neo4jAdapter, auth_info=self.auth_info)

    def tearDown(self):
        [self.test_prov_files[k].close() for k in self.test_prov_files.keys()]
        session = self.provapi._adapter._create_session()
        session.run("MATCH (x) DETACH DELETE x")
        del self.provapi

    # Test create instnace
    def test_provapi_instance(self):
        self.assertRaises(NoDataBaseAdapterException, lambda: ProvApi())
        self.assertRaises(InvalidOptionsException, lambda: ProvApi(api_id=1, adapter=Neo4jAdapter))

        obj = ProvApi(api_id=1, adapter=Neo4jAdapter, auth_info=self.auth_info)
        self.assertIsInstance(obj, ProvApi)
        self.assertEqual(obj.api_id, 1)

        obj = ProvApi(adapter=Neo4jAdapter, auth_info=self.auth_info)
        self.assertIsInstance(obj.api_id, UUID)

    # Methods that automatically convert to ProvDocument
    def test_create_document_from_json(self):
        json_buffer = self.test_prov_files["json"]
        self.provapi.create_document_from_json(json_buffer)

    def test_get_document_as_json(self):
        example = examples.primer_example()
        document_id = self.provapi.create_document_from_prov(example)

        prov_str = self.provapi.get_document_as_json(document_id)
        self.assertIsNotNone(prov_str)
        self.assertIsInstance(prov_str, str)
        prov_document_reverse = ProvDocument.deserialize(content=prov_str, format="json")
        self.assertEqual(prov_document_reverse, example)

    def test_create_document_from_xml(self):
        json_buffer = self.test_prov_files["xml"]
        self.provapi.create_document_from_json(json_buffer)

    def test_get_document_as_xml(self):
        example = examples.primer_example()
        document_id = self.provapi.create_document_from_prov(example)

        prov_str = self.provapi.get_document_as_xml(document_id)
        self.assertIsNotNone(prov_str)
        self.assertIsInstance(prov_str, str)

        prov_document_reverse = ProvDocument.deserialize(content=prov_str, format="xml")
        self.assertEqual(prov_document_reverse, example)

    def test_create_document_from_provn(self):
        json_buffer = self.test_prov_files["provn"]
        with self.assertRaises(NotImplementedError):
            self.provapi.create_document_from_provn(json_buffer)

    def test_get_document_as_provn(self):
        example = examples.primer_example()
        document_id = self.provapi.create_document_from_prov(example)

        prov_str = self.provapi.get_document_as_provn(document_id)
        self.assertIsNotNone(prov_str)
        self.assertIsInstance(prov_str, str)

        # This check throws NotImplementedError, so skip it

        # prov_document_reverse = ProvDocument.deserialize(content=prov_str,format="provn")
        # self.assertEqual(prov_document_reverse, example)

    # Methods with ProvDocument input / output
    def test_create_document(self):
        # test prov document input
        example = examples.primer_example()
        document_id = self.provapi.create_document_from_prov(example)
        self.assertIsNotNone(document_id)
        self.assertIsInstance(document_id, str)

        # test invalid options input
        with self.assertRaises(InvalidArgumentTypeException):
            self.provapi.create_document(1)

    def test_create_document_from_prov(self):
        example = examples.primer_example()
        document_id = self.provapi.create_document_from_prov(example)
        self.assertIsNotNone(document_id)
        self.assertIsInstance(document_id, str)

    def test_create_document_from_prov_alternate(self):
        example = examples.primer_example_alternate()
        document_id = self.provapi.create_document_from_prov(example)
        self.assertIsNotNone(document_id)
        self.assertIsInstance(document_id, str)

    def test_create_document_from_prov_bundles(self):
        example = examples.bundles1()
        document_id = self.provapi.create_document_from_prov(example)
        self.assertIsNotNone(document_id)
        self.assertIsInstance(document_id, str)

    def test_create_document_from_prov_bundles2(self):
        example = examples.bundles2()
        document_id = self.provapi.create_document_from_prov(example)
        self.assertIsNotNone(document_id)
        self.assertIsInstance(document_id, str)

    def test_create_document_from_prov_invalid_arguments(self):
        with self.assertRaises(InvalidArgumentTypeException):
            self.provapi.create_document_from_prov(None)

    def test_get_document_as_prov(self):
        example = examples.bundles2()
        document_id = self.provapi.create_document_from_prov(example)

        prov_document = self.provapi.get_document_as_prov(document_id)
        self.assertIsNotNone(prov_document)
        self.assertIsInstance(prov_document, ProvDocument)

        self.assertEqual(prov_document, example)

    def test_get_document_as_prov_invalid_arguments(self):
        with self.assertRaises(InvalidArgumentTypeException):
            self.provapi.get_document_as_prov()

    def test_create_bundle_invalid_arguments(self):
        with self.assertRaises(InvalidArgumentTypeException):
            self.provapi._create_bundle(None)

    def test_get_metadata_and_attributes_for_record_invalid_arguments(self):
        with self.assertRaises(InvalidArgumentTypeException):
            self.provapi._get_metadata_and_attributes_for_record(None)

    def test_get_metadata_and_attributes_for_record(self):
        example = examples.prov_api_record_example()

        result = self.provapi._get_metadata_and_attributes_for_record(example.prov_record)
        metadata_result = result.metadata
        attributes_result = result.attributes

        self.assertIsNotNone(result.attributes)
        self.assertIsNotNone(result.metadata)
        self.assertIsInstance(result.attributes, dict)
        self.assertIsInstance(result.metadata, dict)

        self.assertIsNotNone(metadata_result[METADATA_KEY_PROV_TYPE])
        self.assertIsNotNone(metadata_result[METADATA_KEY_IDENTIFIER])
        self.assertIsNotNone(metadata_result[METADATA_KEY_NAMESPACES])
        self.assertIsNotNone(metadata_result[METADATA_KEY_TYPE_MAP])

        # check metadata
        self.assertEqual(example.metadata[METADATA_KEY_PROV_TYPE], metadata_result[METADATA_KEY_PROV_TYPE])
        self.assertEqual(example.metadata[METADATA_KEY_IDENTIFIER], metadata_result[METADATA_KEY_IDENTIFIER])
        self.assertEqual(example.metadata[METADATA_KEY_NAMESPACES], metadata_result[METADATA_KEY_NAMESPACES])
        self.assertEqual(example.metadata[METADATA_KEY_TYPE_MAP], metadata_result[METADATA_KEY_TYPE_MAP])

        # check attributes
        self.assertEqual(example.expected_attributes, attributes_result)
