import unittest
from abc import ABC, abstractmethod
from provdbconnector.databases.baseconnector import BaseConnector
from prov.tests.examples import primer_example
from prov.model import ProvRecord, ProvDocument

from tests.examples import base_connector_record_parameter_example,base_connector_relation_parameter_example
class ConnectorTestTemplate(unittest.TestCase):


    def __init__(self, *args, **kwargs):
        """
        Prevent from execute the test case directly see:

        http://stackoverflow.com/questions/4566910/abstract-test-case-using-python-unittest

        :param args:
        :param kwargs:
        """
        super(ConnectorTestTemplate, self).__init__(*args, **kwargs)
        self.helper = None
        # Kludge alert: We want this class to carry test cases without being run
        # by the unit test framework, so the `run' method is overridden to do
        # nothing.  But in order for sub-classes to be able to do something when
        # run is invoked, the constructor will rebind `run' from TestCase.
        if self.__class__ != ConnectorTestTemplate:
            # Rebind `run' from the parent class.
            self.run = unittest.TestCase.run.__get__(self, self.__class__)
        else:
            self.run = lambda self, *args, **kwargs: None

    ### create section ###
    def test_create_document(self):
        id = self.instance.create_document()
        self.assertIsNotNone(id)
        self.assertIs(type(id), str,"id should be a string ")

    def test_create_bundle(self):
        doc_id = self.instance.create_document()
        id = self.instance.create_bundle(doc_id)
        self.assertIsNotNone(id)
        self.assertIs(type(id), str, "id should be a string ")

    def test_create_record(self):
        args = base_connector_record_parameter_example()

        doc_id = self.instance.create_document()
        record_id  = self.instance.create_record(doc_id, args.attributes, args.metadata)
        self.assertIsNotNone(record_id)
        self.assertIs(type(record_id), str, "id should be a string ")

    def test_create_relation(self):
        args = base_connector_relation_parameter_example()

        doc_id = self.instance.create_document()
        record_id = self.instance.create_relation(doc_id, args.attributes, args.metadata)
        self.assertIsNotNone(record_id)
        self.assertIs(type(record_id), str, "id should be a string ")

    ### Get section ###
    def test_get_document(self):
        args = base_connector_record_parameter_example()

        doc_id = self.instance.create_document()
        record_id = self.instance.create_record(doc_id, args.attributes, args.metadata)

        prov_doc = self.instance.get_document(doc_id)


        self.assertIsNotNone(prov_doc)
        self.assertIsInstance(prov_doc,ProvDocument)
        self.assertEqual(prov_doc.get_records(), 1)


        prov_record = prov_doc.get_records().pop()
        self.assertIsInstance(prov_record,ProvRecord)
        self.assertEqual(prov_record, args.prov_record)

    def test_get_bundle(self):
        NotImplementedError()

    def test_get_record(self):
        NotImplementedError()

    def test_get_relation(self):
        NotImplementedError()


    ###Delete section ###
    def test_delete_document(self):
        NotImplementedError()

    def test_delete_bundle(self):
        NotImplementedError()

    def test_delete_record(self):
        NotImplementedError()

    def test_delete_relation(self):
        NotImplementedError()


class BaseConnectorTests(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_instance_abstract_class(self):

        with self.assertRaises(TypeError):
            instance = BaseConnector()

