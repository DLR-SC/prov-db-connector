import unittest
from abc import ABC, abstractmethod
from provdbconnector.databases.baseadapter import BaseAdapter,METADATA_KEY_IDENTIFIER,METADATA_KEY_BUNDLE_ID,METADATA_PARENT_ID, NotFoundException
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

def insert_document_with_bundles(instance):
    args_record = base_connector_record_parameter_example()
    args_bundle = base_connector_bundle_parameter_example()
    doc = ProvDocument()
    doc.add_namespace("ex","http://example.com")
    #document with 1 record
    doc_id = instance.create_document()
    doc_record_id = instance.create_record(doc_id, args_record["attributes"], args_record["metadata"])

    #bundle with 1 record
    bundle_id = instance.create_bundle(doc_id, args_bundle["attributes"], args_bundle["metadata"])
    bundle_record_id = instance.create_record(bundle_id, args_record["attributes"], args_record["metadata"])


    #add relation

    from_record_args = base_connector_record_parameter_example()
    to_record_args = base_connector_record_parameter_example()
    relation_args = base_connector_relation_parameter_example()

    from_label = doc.valid_qualified_name("ex:FROM NODE")
    to_label = doc.valid_qualified_name("ex:TO NODE")
    from_record_args["metadata"][METADATA_KEY_IDENTIFIER] = from_label
    to_record_args["metadata"][METADATA_KEY_IDENTIFIER] = to_label

    from_record_id = instance.create_record(doc_id, from_record_args["attributes"],from_record_args["metadata"])
    to_record_id = instance.create_record(doc_id, to_record_args["attributes"], to_record_args["metadata"])


    relation_id = instance.create_relation(doc_id, from_label,doc_id, to_label, relation_args["attributes"],relation_args["metadata"])



    return {
        "relation_id": relation_id,
        "from_record_id": from_record_id,
        "to_record_id": to_record_id,
        "bundle_id": bundle_id,
        "bundle_record_id": bundle_record_id,
        "doc_id": doc_id,
        "doc_record_id": doc_record_id
    }


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
        from_meta.update({METADATA_KEY_IDENTIFIER: args_relation["from_node"]})
        from_node_id  = self.instance.create_record(doc_id, args_records["attributes"], from_meta)

        to_meta = args_records["metadata"].copy()
        to_meta.update({METADATA_KEY_IDENTIFIER: args_relation["to_node"]})
        to_node_id  = self.instance.create_record(doc_id, args_records["attributes"], to_meta)


        relation_id = self.instance.create_relation(doc_id,args_relation["from_node"],doc_id,args_relation["to_node"], args_relation["attributes"], args_relation["metadata"])
        self.assertIsNotNone(relation_id)
        self.assertIs(type(relation_id), str, "id should be a string ")


    @unittest.skip("We should discuss the possibility of relations that are create automatically nodes")
    def test_create_relation_with_unknown_records(self):
        args_relation = base_connector_relation_parameter_example()

        doc_id = self.instance.create_document()

        #Skip the part that creates the from and to node

        relation_id = self.instance.create_relation(doc_id,args_relation["from_node"],doc_id,args_relation["to_node"], args_relation["attributes"], args_relation["metadata"])

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

        attrDict = encode_dict_values_to_primitive(args["attributes"])
        metaDict = encode_dict_values_to_primitive(args["metadata"])

        #add bundle_id to expected meta_data
        metaDict.update({METADATA_KEY_BUNDLE_ID: raw_doc.document.records[0].metadata[METADATA_KEY_BUNDLE_ID]})

        self.assertEqual(raw_doc.document.records[0].attributes,attrDict )
        self.assertEqual(raw_doc.document.records[0].metadata, metaDict)


        #check bundle
        self.assertIsInstance(raw_doc.bundles,list)
        self.assertEqual(len(raw_doc.bundles),0)

    def test_get_document_with_budles(self):
        ids = insert_document_with_bundles(self.instance)
        raw_doc = self.instance.get_document(ids["doc_id"])



        #check document
        self.assertIsNotNone(raw_doc)
        self.assertIsNotNone(raw_doc.document)
        self.assertIsInstance(raw_doc.document.records, list)
        self.assertIsInstance(raw_doc.document.records[0].attributes, dict)
        self.assertIsInstance(raw_doc.document.records[0].metadata, dict)
        #3 records + 1 relation
        self.assertEqual(len(raw_doc.document.records), 4)

        # check bundle
        self.assertIsInstance(raw_doc.bundles, list)
        self.assertEqual(len(raw_doc.bundles), 1)
        #1 record
        self.assertEqual(len(raw_doc.bundles[0].records), 1)



    def test_get_document_not_found(self):
        with self.assertRaises(NotFoundException):
            self.instance.get_document("99999999")

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
        self.assertIsNotNone(raw_bundle.bundle_record)
        self.assertIsNotNone(raw_bundle.bundle_record.metadata)
        self.assertIsNotNone(raw_bundle.bundle_record.attributes)
        self.assertEqual(raw_bundle.identifier, str(args_bundle["metadata"][METADATA_KEY_IDENTIFIER]))
        self.assertIsInstance(raw_bundle.records, list)
        self.assertIsInstance(raw_bundle.bundle_record.attributes, dict)
        self.assertIsInstance(raw_bundle.bundle_record.metadata, dict)
        self.assertEqual(len(raw_bundle.records), 1)

        #check parent document id information
        parent_id = raw_bundle.bundle_record.metadata[METADATA_PARENT_ID]
        self.assertEqual(doc_id,parent_id, "parent id should be the document id")

        #check if the metadata of the record equals
        attrDict = encode_dict_values_to_primitive(args["attributes"])
        metaDict = encode_dict_values_to_primitive(args["metadata"])

        # add bundle_id to expected meta_data
        metaDict.update({METADATA_KEY_BUNDLE_ID: raw_bundle.records[0].metadata[METADATA_KEY_BUNDLE_ID]})

        self.assertEqual(raw_bundle.records[0].attributes, attrDict)
        self.assertEqual(raw_bundle.records[0].metadata, metaDict)

        # check bundle

    def test_get_bundle_not_found(self):
        with self.assertRaises(NotFoundException):
            self.instance.get_bundle("99999999")

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
        metadata_primitive= encode_dict_values_to_primitive(args["metadata"])

        # add bundle_id to expected meta_data
        metadata_primitive.update({METADATA_KEY_BUNDLE_ID: record_raw.metadata[METADATA_KEY_BUNDLE_ID]})

        self.assertEqual(record_raw.attributes,attributes_primitive)
        self.assertEqual(record_raw.metadata,metadata_primitive)

        self.assertIs(type(record_id), str, "id should be a string ")

    def test_get_record_not_found(self):
        with self.assertRaises(NotFoundException):
            self.instance.get_record("99999999")

    def test_get_relation(self):
        from_record_args = base_connector_record_parameter_example()
        to_record_args = base_connector_record_parameter_example()
        relation_args = base_connector_relation_parameter_example()

        from_identifier = relation_args["from_node"]
        to_identifier = relation_args["to_node"]
        from_record_args["metadata"][METADATA_KEY_IDENTIFIER] = from_identifier
        to_record_args["metadata"][METADATA_KEY_IDENTIFIER] = to_identifier

        doc_id = self.instance.create_document()
        from_record_id = self.instance.create_record(doc_id, from_record_args["attributes"], from_record_args["metadata"])  #
        to_record_id = self.instance.create_record(doc_id, to_record_args["attributes"], to_record_args["metadata"])  #

        relation_id = self.instance.create_relation(doc_id,from_identifier,doc_id,to_identifier,relation_args["attributes"], relation_args["metadata"])

        relation_raw = self.instance.get_relation(relation_id)

        self.assertIsNotNone(relation_raw)
        self.assertIsNotNone(relation_raw.attributes)
        self.assertIsNotNone(relation_raw.metadata)
        self.assertIsInstance(relation_raw.metadata, dict)
        self.assertIsInstance(relation_raw.attributes, dict)

        attributes_primitive = encode_dict_values_to_primitive(relation_args["attributes"])
        metadata_primitive = encode_dict_values_to_primitive(relation_args["metadata"])

        # add bundle_id to expected meta_data
        metadata_primitive.update({METADATA_KEY_BUNDLE_ID: relation_raw.metadata[METADATA_KEY_BUNDLE_ID]})

        self.assertEqual(relation_raw.attributes, attributes_primitive)
        self.assertEqual(relation_raw.metadata, metadata_primitive)

    def test_get_relation_not_found(self):
        with self.assertRaises(NotFoundException):
            self.instance.get_relation("99999999")

    ##Delete section ###
    def test_delete_document(self):
        ids = insert_document_with_bundles(self.instance)
        doc_id  = ids["doc_id"]
        result = self.instance.delete_document(doc_id)
        self.assertIsInstance(result,bool)
        self.assertTrue(result)

        with self.assertRaises(NotFoundException):
            self.instance.get_document(doc_id)

    def test_delete_bundle(self):
        ids = insert_document_with_bundles(self.instance)
        bundle_id  = ids["bundle_id"]
        result = self.instance.delete_bundle(bundle_id)
        self.assertIsInstance(result,bool)
        self.assertTrue(result)

        with self.assertRaises(NotFoundException):
            self.instance.get_bundle(bundle_id)

    def test_delete_record(self):
        ids = insert_document_with_bundles(self.instance)
        from_record_id = ids["from_record_id"]
        result = self.instance.delete_record(from_record_id)
        self.assertIsInstance(result, bool)
        self.assertTrue(result)

        with self.assertRaises(NotFoundException):
            self.instance.get_record(from_record_id)

    def test_delete_relation(self):
        ids = insert_document_with_bundles(self.instance)
        relation_id = ids["relation_id"]
        result = self.instance.delete_relation(relation_id)
        self.assertIsInstance(result, bool)
        self.assertTrue(result)

        with self.assertRaises(NotFoundException):
            self.instance.get_relation(relation_id)


class BaseConnectorTests(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_instance_abstract_class(self):

        with self.assertRaises(TypeError):
            instance = BaseAdapter()

