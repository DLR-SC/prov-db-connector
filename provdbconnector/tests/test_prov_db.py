import unittest
from uuid import UUID

import pkg_resources
from prov.model import ProvDocument, ProvAgent, ProvEntity, ProvActivity, QualifiedName, ProvRelation, ProvRecord, ProvBundle

from provdbconnector.tests import examples as examples
from provdbconnector import ProvDb
from provdbconnector.exceptions.database import InvalidOptionsException, NotFoundException
from provdbconnector import Neo4jAdapter, NEO4J_USER, NEO4J_PASS, NEO4J_HOST, NEO4J_BOLT_PORT
from provdbconnector.db_adapters.baseadapter import METADATA_KEY_TYPE_MAP, METADATA_KEY_PROV_TYPE, \
    METADATA_KEY_IDENTIFIER, METADATA_KEY_NAMESPACES, METADATA_KEY_IDENTIFIER_ORIGINAL
from provdbconnector.exceptions.provapi import NoDataBaseAdapterException, InvalidArgumentTypeException


class ProvDbTestTemplate(unittest.TestCase):
    """
    This abstract test class to test the high level function of you database adapter.
    To use this unitest Template extend from this class.

    .. literalinclude:: ../provdbconnector/tests/db_adapters/in_memory/test_simple_in_memory.py
       :linenos:
       :language: python
       :lines: 25-40

    """
    def __init__(self, *args, **kwargs):
        """
        Prevent from execute the test case directly see:

        http://stackoverflow.com/questions/4566910/abstract-test-case-using-python-unittest

        :param args:
        :param kwargs:
        """
        super(ProvDbTestTemplate, self).__init__(*args, **kwargs)
        self.helper = None
        # Kludge alert: We want this class to carry test cases without being run
        # by the unit test framework, so the `run' method is overridden to do
        # nothing.  But in order for sub-classes to be able to do something when
        # run is invoked, the constructor will rebind `run' from TestCase.
        if self.__class__ != ProvDbTestTemplate:
            # Rebind `run' from the parent class.
            self.run = unittest.TestCase.run.__get__(self, self.__class__)
        else:
            self.run = lambda self, *args, **kwargs: None

    def setUp(self):
        """
        Use the setup method to create a provapi instance with you adapter

        .. warning::
            Override this function if you extend this test!
            Otherwise the test will fail.

        :return:
        """
        # this function will never be executed !!!!
        self.provapi = ProvDb()

    def clear_database(self):
        """
        Override this function to clear your database before each test

        :return:
        """
        raise InvalidArgumentTypeException()



    def test_prov_primer_example(self):
        """
        This test try to save and restore a common prov example document

        :return:
        """
        self.clear_database()
        prov_document = examples.primer_example()
        stored_document_id = self.provapi.save_document_from_prov(prov_document)
        stored_document = self.provapi.get_document_as_prov(stored_document_id)

        self.assertEqual(stored_document, prov_document)

    def test_primer_example_alternate(self):
        """
        This test try to save and restore a common prov example document.
        But in a more complex way

        :return:
        """
        self.clear_database()
        prov_document = examples.primer_example_alternate()
        stored_document_id = self.provapi.save_document_from_prov(prov_document)
        stored_document = self.provapi.get_document_as_prov(stored_document_id)

        self.assertEqual(stored_document, prov_document)

    def test_w3c_publication_1(self):
        """
        This test try to save and restore a common prov example document.

        :return:
        """
        self.clear_database()
        prov_document = examples.w3c_publication_1()
        stored_document_id = self.provapi.save_document_from_prov(prov_document)
        stored_document = self.provapi.get_document_as_prov(stored_document_id)

        self.assertEqual(stored_document, prov_document)

    def test_w3c_publication_2(self):
        """
        This test try to save and restore a common prov example document.

        :return:
        """

        self.clear_database()
        prov_document = examples.w3c_publication_2()
        stored_document_id = self.provapi.save_document_from_prov(prov_document)
        stored_document = self.provapi.get_document_as_prov(stored_document_id)

        self.assertEqual(stored_document, prov_document)

    def test_bundles1(self):
        """
        This test try to save and restore a common prov example document.
        With a bundle and some connections inside the bundle.
        This example is also available via `Provstore <https://provenance.ecs.soton.ac.uk/store/documents/114710/>`

        :return:
        """
        self.clear_database()
        prov_document = examples.bundles1()
        stored_document_id = self.provapi.save_document_from_prov(prov_document)
        stored_document = self.provapi.get_document_as_prov(stored_document_id)
        stored_document_unified = stored_document.flattened().unified()
        prov_document_unified= prov_document.flattened().unified()
        self.assertEqual(stored_document_unified, prov_document_unified)

    def test_bundles2(self):
        """
        This test try to save and restore a common prov example document.
        With a bundle and some connections inside the bundle.
        This example is also available via `Provstore <https://provenance.ecs.soton.ac.uk/store/documents/114704/>`

        The main difference to the bundle_1 is that here we have also a mentionOf connection between bundles.
        See PROV-Links spec for more information

        :return:
        """
        self.clear_database()
        prov_document = examples.bundles2()
        stored_document_id = self.provapi.save_document_from_prov(prov_document)
        stored_document = self.provapi.get_document_as_prov(stored_document_id)

        self.assertEqual(stored_document, prov_document)

    def test_collections(self):
        """
        This test try to save and restore a common prov example document.

        :return:
        """

        self.clear_database()
        prov_document = examples.collections()
        stored_document_id = self.provapi.save_document_from_prov(prov_document)
        stored_document = self.provapi.get_document_as_prov(stored_document_id)

        self.assertEqual(stored_document, prov_document)

    def test_long_literals(self):
        """
        This test try to save and restore a common prov example document.

        :return:
        """

        self.clear_database()
        prov_document = examples.long_literals()
        stored_document_id = self.provapi.save_document_from_prov(prov_document)
        stored_document = self.provapi.get_document_as_prov(stored_document_id)

        self.assertEqual(stored_document, prov_document)

    def test_datatypes(self):
        """
        This test try to save and restore a common prov example document.

        :return:
        """

        self.clear_database()
        prov_document = examples.datatypes()
        stored_document_id = self.provapi.save_document_from_prov(prov_document)
        stored_document = self.provapi.get_document_as_prov(stored_document_id)

        self.assertEqual(stored_document, prov_document)


class ProvDbTests(unittest.TestCase):
    """
    This tests are only for the ProvDb itself. You don't have to extend this test in case you want to write your own
    adapter
    """
    maxDiff = None

    def setUp(self):
        """
        Loads the test xml json and provn data

        """
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
        self.provapi = ProvDb(api_id=1, adapter=Neo4jAdapter, auth_info=self.auth_info)

    def clear_database(self):
        session = self.provapi._adapter._create_session()
        session.run("MATCH (x) DETACH DELETE x")

    def tearDown(self):
        """
        Destroy the prov api and remove all data from neo4j
        :return:
        """
        [self.test_prov_files[k].close() for k in self.test_prov_files.keys()]
        self.clear_database()
        del self.provapi

    # Test create instnace
    def test_provapi_instance(self):
        """
        Try to create a test instnace
        :return:
        """
        self.assertRaises(NoDataBaseAdapterException, lambda: ProvDb())
        self.assertRaises(InvalidOptionsException, lambda: ProvDb(api_id=1, adapter=Neo4jAdapter))

        obj = ProvDb(api_id=1, adapter=Neo4jAdapter, auth_info=self.auth_info)
        self.assertIsInstance(obj, ProvDb)
        self.assertEqual(obj.api_id, 1)

        obj = ProvDb(adapter=Neo4jAdapter, auth_info=self.auth_info)
        self.assertIsInstance(obj.api_id, UUID)

    # Methods that automatically convert to ProvDocument
    def test_save_document_from_json(self):
        """
        Try to create a document from a json buffer
        :return:
        """
        self.clear_database()
        json_buffer = self.test_prov_files["json"]
        self.provapi.save_document_from_json(json_buffer)

    def test_get_document_as_json(self):
        """
        try to get the document as json
        :return:
        """
        self.clear_database()
        example = examples.primer_example()
        document_id = self.provapi.save_document_from_prov(example)

        prov_str = self.provapi.get_document_as_json(document_id)
        self.assertIsNotNone(prov_str)
        self.assertIsInstance(prov_str, str)
        prov_document_reverse = ProvDocument.deserialize(content=prov_str, format="json")
        self.assertEqual(prov_document_reverse, example)

    def test_save_document_from_xml(self):
        """
        Try to create a document from xml
        :return:
        """
        self.clear_database()
        json_buffer = self.test_prov_files["xml"]
        self.provapi.save_document_from_json(json_buffer)

    def test_get_document_as_xml(self):
        """
        try to get the document as xml
        :return:
        """
        self.clear_database()
        example = examples.primer_example()
        document_id = self.provapi.save_document_from_prov(example)

        prov_str = self.provapi.get_document_as_xml(document_id)
        self.assertIsNotNone(prov_str)
        self.assertIsInstance(prov_str, str)

        prov_document_reverse = ProvDocument.deserialize(content=prov_str, format="xml")
        self.assertEqual(prov_document_reverse, example)

    def test_save_document_from_provn(self):
        """
        Try to create a document from provn
        :return:
        """
        self.clear_database()
        json_buffer = self.test_prov_files["provn"]
        with self.assertRaises(NotImplementedError):
            self.provapi.save_document_from_provn(json_buffer)

    def test_get_document_as_provn(self):
        """
        Try to get a document in provn
        :return:
        """
        self.clear_database()
        example = examples.primer_example()
        document_id = self.provapi.save_document_from_prov(example)

        prov_str = self.provapi.get_document_as_provn(document_id)
        self.assertIsNotNone(prov_str)
        self.assertIsInstance(prov_str, str)

        # This check throws NotImplementedError, so skip it

        # prov_document_reverse = ProvDocument.deserialize(content=prov_str,format="provn")
        # self.assertEqual(prov_document_reverse, example)

    # Methods with ProvDocument input / output
    def test_save_document(self):
        """
        Try to create a document from a prov instnace
        :return:
        """
        self.clear_database()
        # test prov document input
        example = examples.primer_example()
        document_id = self.provapi.save_document_from_prov(example)
        self.assertIsNotNone(document_id)
        self.assertIsInstance(document_id, str)

        # test invalid options input
        with self.assertRaises(InvalidArgumentTypeException):
            self.provapi.save_document(1)

    def test_save_document_from_prov(self):
        """
        Try to create a primer example document
        :return:
        """
        self.clear_database()
        example = examples.primer_example()
        document_id = self.provapi.save_document_from_prov(example)
        self.assertIsNotNone(document_id)
        self.assertIsInstance(document_id, str)

    def test_save_document_from_prov_alternate(self):
        """
        Try to create a prov_alternative
        :return:
        """
        self.clear_database()
        example = examples.primer_example_alternate()
        document_id = self.provapi.save_document_from_prov(example)
        self.assertIsNotNone(document_id)
        self.assertIsInstance(document_id, str)

    def test_save_document_from_prov_bundles(self):
        """
        Try to create a document with bundles
        :return:
        """
        self.clear_database()
        example = examples.bundles1()
        document_id = self.provapi.save_document_from_prov(example)
        self.assertIsNotNone(document_id)
        self.assertIsInstance(document_id, str)

    def test_save_document_from_prov_bundles2(self):
        """
        Try to create more bundles
        :return:
        """
        self.clear_database()
        example = examples.bundles2()
        document_id = self.provapi.save_document_from_prov(example)
        self.assertIsNotNone(document_id)
        self.assertIsInstance(document_id, str)

    def test_save_document_from_prov_invalid_arguments(self):
        """
        Try to create a prov with some invalid arguments
        :return:
        """
        self.clear_database()
        with self.assertRaises(InvalidArgumentTypeException):
            self.provapi.save_document_from_prov(None)

    def test_get_document_as_prov(self):
        """
        Try to get the document as ProvDocument instnace

        :return:
        """
        self.clear_database()
        example = examples.bundles2()
        document_id = self.provapi.save_document_from_prov(example)

        prov_document = self.provapi.get_document_as_prov(document_id)
        self.assertIsNotNone(prov_document)
        self.assertIsInstance(prov_document, ProvDocument)

        self.assertEqual(prov_document, example)

    def test_get_document_as_prov_invalid_arguments(self):
        """
        Try to get the prov document with invalid arguments

        :return:
        """
        with self.assertRaises(InvalidArgumentTypeException):
            self.provapi.get_document_as_prov()

    def test_save_bundle_invalid_arguments(self):
        """
        Try to create a bundle with invalid arguments
        :return:
        """
        with self.assertRaises(InvalidArgumentTypeException):
            self.provapi._save_bundle_internal(None)

    def test_save_element_invalid(self):
        """
        Test save_element with invalid args

        """

        with self.assertRaises(InvalidArgumentTypeException):
            self.provapi.save_element("Some cool invalid argument")

    def test_save_record(self):
        """
        Test to save a record (a element or a relation)
        """
        self.clear_database()
        doc = examples.primer_example()
        element = list(doc.get_records(ProvActivity)).pop()
        relation = list(doc.get_records(ProvRelation)).pop()

        #save element
        self.provapi.save_record(element)
        db_element = self.provapi.get_element(element.identifier)

        self.assertEqual(db_element,element)

        #Save relation
        self.provapi.save_record(relation)

        # @todo discuss if it a get_relation method is useful, see  https://github.com/DLR-SC/prov-db-connector/issues/45
        # db_relation = self.provapi.get_relation(relation.identifier)
        # self.assertEqual(db_element, element)

    def test_save_record_invalid(self):
        with self.assertRaises(InvalidArgumentTypeException):
            self.provapi.save_record(list())

        with self.assertRaises(InvalidArgumentTypeException):
            self.provapi.save_record(examples.primer_example())

        class InvalidProvRecordExtend(ProvRecord):
            pass

        with self.assertRaises(InvalidArgumentTypeException):
            doc = examples.primer_example()
            identifier = doc.valid_qualified_name("prov:example")
            self.provapi.save_record(InvalidProvRecordExtend(bundle=doc,identifier=identifier))

    def test_save_element(self):
        """
        Try to save a single record without document_di

        """
        self.clear_database()

        doc = examples.primer_example()
        agent = list(doc.get_records(ProvAgent)).pop()
        entity = list(doc.get_records(ProvEntity)).pop()
        activity = list(doc.get_records(ProvActivity)).pop()

        #Try to save the 3 class types
        self.provapi.save_element(agent)
        self.provapi.save_element(entity)
        self.provapi.save_element(activity)

    def test_get_elements(self):
        """
        Test for the get_elements function
        """
        self.clear_database()
        doc = examples.bundles2()
        self.provapi.save_document(doc)

        activities = self.provapi.get_elements(ProvActivity)
        self.assertIsNotNone(activities)
        self.assertIsInstance(activities,ProvDocument)
        self.assertEqual(len(activities.get_records()),0) # document contains only 4 unknown activities

        entities = self.provapi.get_elements(ProvEntity)
        self.assertIsNotNone(entities)
        self.assertIsInstance(entities, ProvDocument)
        self.assertEqual(len(entities.get_records()), 5)

        agents = self.provapi.get_elements(ProvAgent)
        self.assertIsNotNone(agents)
        self.assertIsInstance(agents, ProvDocument)
        self.assertEqual(len(agents.get_records()), 2)

    def test_get_element_invalid(self):
        """
        Test get element with error

        """

        with self.assertRaises(InvalidArgumentTypeException):
            self.provapi.get_element(None)


        with self.assertRaises(NotFoundException):
            doc = ProvDocument()
            name = doc.valid_qualified_name("prov:Some unused name")
            self.provapi.get_element(name)

    def test_get_element(self):
        """
        Try to save a single record without document_id
        and get the record back from the db

        """
        self.clear_database()
        doc = examples.primer_example()
        agent = list(doc.get_records(ProvAgent)).pop()
        entity = list(doc.get_records(ProvEntity)).pop()
        activity = list(doc.get_records(ProvActivity)).pop()

        #Try to save the 3 class types
        agent_id = self.provapi.save_element(agent)
        entity_id = self.provapi.save_element(entity)
        activity_id = self.provapi.save_element(activity)

        self.assertIsInstance(agent_id,QualifiedName)
        self.assertIsInstance(entity_id,QualifiedName)
        self.assertIsInstance(activity_id,QualifiedName)

        agent_restored = self.provapi.get_element(agent_id)
        entity_restored = self.provapi.get_element(entity_id)
        activity_restored = self.provapi.get_element(activity_id)

        self.assertEqual(agent_restored,agent)
        self.assertEqual(entity_restored,entity)
        self.assertEqual(activity_restored,activity)

    def test_save_bundle(self):
        """
        Test the public method to save bundles
        """
        self.clear_database()
        doc = examples.bundles2()
        bundle = list(doc.bundles).pop()

        self.provapi.save_bundle(bundle)

    def test_save_bundle_invalid(self):
        """
        Test the public method to save bundles with invalid arguments
        """
        doc = examples.primer_example()

        with self.assertRaises(InvalidArgumentTypeException):
            self.provapi.save_bundle(doc)

        with self.assertRaises(InvalidArgumentTypeException):
            self.provapi.save_bundle(None)

    def test_get_bundle(self):
        """
        Test the public method to get bundles
        """
        self.clear_database()
        doc = examples.bundles2()
        bundle = list(doc.bundles).pop()

        self.provapi.save_bundle(bundle)

        db_bundle = self.provapi.get_bundle(bundle.identifier)
        self.assertIsNotNone(db_bundle)
        self.assertIsInstance(db_bundle,ProvBundle)
        self.assertEqual(db_bundle,bundle)

    def test_get_bundle_invalid(self):
        """
        Test with invalid arguemnts
        """

        with self.assertRaises(InvalidArgumentTypeException):
            self.provapi.get_bundle("prov:str") #not allowed
        with self.assertRaises(InvalidArgumentTypeException):
            self.provapi.get_bundle(None)  # not allowed

    def test_save_relation_with_unknown_nodes(self):
        """
        Test to create a relation were the start and end node dose not exist
        This should also work
        """
        self.clear_database()
        doc = examples.primer_example()
        relation = list(doc.get_records(ProvRelation)).pop()

        self.provapi.save_relation(relation)

    def test_save_relation_invalid(self):

        with self.assertRaises(InvalidArgumentTypeException):
            self.provapi.save_relation(None)


        with self.assertRaises(InvalidArgumentTypeException):
            doc = examples.primer_example()
            element = list(doc.get_records(ProvEntity)).pop()
            self.provapi.save_relation(element)

    def test_get_metadata_and_attributes_for_record_invalid_arguments(self):
        """
        Try to get attributes and metadata with invalid arguments
        :return:
        """
        with self.assertRaises(InvalidArgumentTypeException):
            self.provapi._get_metadata_and_attributes_for_record(None)

    def test_save_unknown_prov_typ(self):
        """
        Test to prefer non unknown prov type
        """
        self.clear_database()
        doc = examples.prov_db_unknown_prov_typ_example()
        self.provapi.save_document(doc)
        doc_with_entities = self.provapi.get_elements(ProvEntity)
        self.assertEqual(len(doc_with_entities.records), 2)
        self.assertIsInstance(doc_with_entities.records[0], ProvEntity)
        self.assertIsInstance(doc_with_entities.records[1], ProvEntity)

    def test_save_with_override_default_namespace(self):
        """
        Test to support default namespace overrides
        """
        self.clear_database()
        docA = examples.prov_default_namespace_example("docA")
        docB = examples.prov_default_namespace_example("docB")
        self.provapi.save_document(docA)
        self.provapi.save_document(docB)
        doc_with_entities = self.provapi.get_elements(ProvEntity)
        self.assertEqual(len(doc_with_entities.records), 2)

    def test_get_metadata_and_attributes_for_record(self):
        """
        Test the split into metadata / attributes function
        This function separates the attributes and metadata from a prov record
        :return:
        """
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
        self.assertEqual(example.metadata[METADATA_KEY_IDENTIFIER], metadata_result[METADATA_KEY_IDENTIFIER_ORIGINAL])
        self.assertEqual(example.metadata[METADATA_KEY_NAMESPACES], metadata_result[METADATA_KEY_NAMESPACES])
        self.assertEqual(example.metadata[METADATA_KEY_TYPE_MAP], metadata_result[METADATA_KEY_TYPE_MAP])

        identifier = example.metadata[METADATA_KEY_IDENTIFIER]
        self.assertEqual(identifier.namespace.uri + identifier.localpart, metadata_result[METADATA_KEY_IDENTIFIER])
        # check attributes
        self.assertEqual(example.expected_attributes, attributes_result)
