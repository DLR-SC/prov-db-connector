import unittest
from abc import ABC, abstractmethod
from provdbconnector.databases.baseadapter import BaseAdapter,METADATA_KEY_LABEL,METADATA_KEY_BUNDLE_ID
from prov.tests.examples import primer_example
from prov.model import ProvRecord, ProvDocument
from provdbconnector.utils.serializer import encode_string_value_to_primitive,encode_dict_values_to_primitive

from tests.examples import base_connector_record_parameter_example,base_connector_relation_parameter_example,base_connector_bundle_parameter_example

def isnamedtupleinstance(x):
    t = type(x)
    b = t.__bases__
    if len(b) != 1 or b[0] != tuple: return False
    f = getattr(t, '_fields', None)
    if not isinstance(f, tuple): return False
    return all(type(n)==str for n in f)

class AdapterTestTemplate(unittest.TestCase):


    def __init__(self, *args, **kwargs):
        """
        Prevent from execute the test case directly see:

        http://stackoverflow.com/questions/4566910/abstract-test-case-using-python-unittest

        :param args:
        :param kwargs:
        """
        super(AdapterTestTemplate, self).__init__(*args, **kwargs)
        self.helper = None
        # Kludge alert: We want this class to carry test cases without being run
        # by the unit test framework, so the `run' method is overridden to do
        # nothing.  But in order for sub-classes to be able to do something when
        # run is invoked, the constructor will rebind `run' from TestCase.
        if self.__class__ != AdapterTestTemplate:
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
        args = base_connector_bundle_parameter_example()
        id = self.instance.create_bundle(doc_id, args["attributes"], args["metadata"] )
        self.assertIsNotNone(id)
        self.assertIs(type(id), str, "id should be a string ")

    def test_create_record(self):
        args = base_connector_record_parameter_example()

        doc_id = self.instance.create_document()
        record_id  = self.instance.create_record(doc_id, args["attributes"], args["metadata"])
        self.assertIsNotNone(record_id)
        self.assertIs(type(record_id), str, "id should be a string ")

    def test_create_relation(self):
        args_relation = base_connector_relation_parameter_example()
        args_records = base_connector_record_parameter_example()
        doc_id = self.instance.create_document()

        from_meta = args_records["metadata"].copy()
        from_meta.update({METADATA_KEY_LABEL: args_relation["from_node"]})
        from_node_id  = self.instance.create_record(doc_id, args_records["attributes"], from_meta)

        to_meta = args_records["metadata"].copy()
        to_meta.update({METADATA_KEY_LABEL: args_relation["to_node"]})
        to_node_id  = self.instance.create_record(doc_id, args_records["attributes"], to_meta)


        relation_id = self.instance.create_relation(doc_id,args_relation["from_node"],args_relation["to_node"], args_relation["attributes"], args_relation["metadata"])
        self.assertIsNotNone(relation_id)
        self.assertIs(type(relation_id), str, "id should be a string ")


    @unittest.skip("We should discuss the possibility of relations that are create automatically nodes")
    def test_create_relation_with_unknown_records(self):
        args_relation = base_connector_relation_parameter_example()

        doc_id = self.instance.create_document()

        #Skip the part that creates the from and to node

        relation_id = self.instance.create_relation(doc_id,args_relation["from_node"],args_relation["to_node"], args_relation["attributes"], args_relation["metadata"])

        self.assertIsNotNone(relation_id)
        self.assertIs(type(relation_id), str, "id should be a string ")

    ### Get section ###
    def test_get_document(self):
        args = base_connector_record_parameter_example()

        doc_id = self.instance.create_document()
        record_id = self.instance.create_record(doc_id, args["attributes"], args["metadata"])

        raw_doc = self.instance.get_document(doc_id)

        # Return structure...
        # raw_doc = {
        #     document: {identifer: "undefied if document", records: [{attributes:{},metadata: {}}]}
        #     bundles: [{identifer: "name bundle", records: [{attributes:{},metadata: {}}]}]
        # }

        self.assertIsNotNone(raw_doc )
        self.assertIsNotNone(raw_doc.document )
        self.assertIsInstance(raw_doc.document.records,list)
        self.assertEqual(len(raw_doc.document.records),1)
        self.assertIsInstance(raw_doc.document.records[0].attributes,dict)
        self.assertIsInstance(raw_doc.document.records[0].metadata,dict)


        attrDict = args["attributes"].copy()
        for key,value in attrDict.items():
            attrDict[key] = encode_string_value_to_primitive(value)

        metaDict = args["metadata"].copy()
        for key, value in metaDict .items():
            metaDict[key] = encode_string_value_to_primitive(value)

        #add bundle_id to expected meta_data
        metaDict.update({METADATA_KEY_BUNDLE_ID: raw_doc.document.records[0].metadata[METADATA_KEY_BUNDLE_ID]})

        self.assertEqual(raw_doc.document.records[0].attributes,attrDict )
        self.assertEqual(raw_doc.document.records[0].metadata, metaDict)


        #check bundle
        self.assertIsInstance(raw_doc.bundles,list)
        self.assertEqual(len(raw_doc.bundles),0)


    def test_get_bundle(self):
        args = base_connector_record_parameter_example()
        args_bundle = base_connector_bundle_parameter_example()

        doc_id = self.instance.create_document()
        bundle_id= self.instance.create_bundle(doc_id,args_bundle["attributes"],args_bundle["metadata"])

        record_id = self.instance.create_record(bundle_id, args["attributes"], args["metadata"])

        raw_bundle = self.instance.get_bundle(bundle_id)

        # Return structure...
        # raw_doc = {
        #     records: []
        #     identifer: ""
        # }

        # check bundle
        self.assertIsNotNone(raw_bundle)
        self.assertIsNotNone(raw_bundle.identifier)
        self.assertEqual(raw_bundle.identifier,args_bundle["metadata"][METADATA_KEY_LABEL])
        self.assertIsInstance(raw_bundle.records, list)
        self.assertEqual(len(raw_bundle.records), 1)


        attrDict = args["attributes"].copy()
        for key, value in attrDict.items():
            attrDict[key] = encode_string_value_to_primitive(value)

        metaDict = args["metadata"].copy()
        for key, value in metaDict.items():
            metaDict[key] = encode_string_value_to_primitive(value)

        # add bundle_id to expected meta_data
        metaDict.update({METADATA_KEY_BUNDLE_ID: raw_bundle.records[0].metadata[METADATA_KEY_BUNDLE_ID]})

        self.assertEqual(raw_bundle.records[0].attributes, attrDict)
        self.assertEqual(raw_bundle.records[0].metadata, metaDict)

        # check bundle

    def test_get_record(self):
        args = base_connector_record_parameter_example()

        doc_id = self.instance.create_document()
        record_id = self.instance.create_record(doc_id, args["attributes"], args["metadata"])#

        record_raw = self.instance.get_record(record_id)

        self.assertIsNotNone(record_raw)
        self.assertIsNotNone(record_raw.attributes)
        self.assertIsNotNone(record_raw.metadata)
        self.assertIsInstance(record_raw.metadata,dict)
        self.assertIsInstance(record_raw.attributes,dict)


        attributes_primitive= encode_dict_values_to_primitive(args["attributes"])
        metadata_primitive= encode_dict_values_to_primitive(args["attributes"])


        self.assertEqual(record_raw.attributes,attributes_primitive)
        self.assertEqual(record_raw.metadata,metadata_primitive)

        self.assertIs(type(record_id), str, "id should be a string ")

    def test_get_relation(self):
        raise NotImplementedError()


    ###Delete section ###
    def test_delete_document(self):
        raise NotImplementedError()

    def test_delete_bundle(self):
        raise NotImplementedError()

    def test_delete_record(self):
        raise NotImplementedError()

    def test_delete_relation(self):
        raise NotImplementedError()


class BaseConnectorTests(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_instance_abstract_class(self):

        with self.assertRaises(TypeError):
            instance = BaseAdapter()

