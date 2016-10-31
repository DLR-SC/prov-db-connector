import unittest

from prov.model import ProvDocument
from prov.constants import PROV_ENTITY, PROV_TYPE,PROV_RECORD_IDS_MAP

from provdbconnector.db_adapters.baseadapter import BaseAdapter, METADATA_KEY_IDENTIFIER, METADATA_KEY_TYPE_MAP, METADATA_KEY_NAMESPACES, METADATA_KEY_PROV_TYPE
from provdbconnector.exceptions.database import NotFoundException, InvalidOptionsException,MergeException
from provdbconnector.tests.examples import base_connector_record_parameter_example, primer_example,\
    base_connector_relation_parameter_example, base_connector_bundle_parameter_example, base_connector_merge_example
from provdbconnector.utils.serializer import encode_dict_values_to_primitive


def isnamedtupleinstance(x):
    t = type(x)
    b = t.__bases__
    if len(b) != 1 or b[0] != tuple:
        return False
    f = getattr(t, '_fields', None)
    if not isinstance(f, tuple):
        return False
    return all(type(n) == str for n in f)

def encode_adapter_result_to_excpect(dict_vals):
    copy = dict_vals.copy()
    copy.update({METADATA_KEY_NAMESPACES: copy[METADATA_KEY_NAMESPACES].pop()})
    copy.update({METADATA_KEY_TYPE_MAP: copy[METADATA_KEY_TYPE_MAP].pop()})

    return encode_dict_values_to_primitive(copy)

def insert_document_with_bundles(instance, identifier_prefix=""):
    args_record = base_connector_record_parameter_example()
    args_bundle = base_connector_bundle_parameter_example()
    doc = ProvDocument()
    doc.add_namespace("ex", "http://example.com")
    # document with 1 record

    args_record["metadata"].update({METADATA_KEY_IDENTIFIER: doc.valid_qualified_name("ex:" + identifier_prefix + str(args_record["metadata"][METADATA_KEY_IDENTIFIER]))})
    doc_record_id = instance.save_record(args_record["attributes"], args_record["metadata"])

    #bundle with 1 record
    args_bundle["metadata"].update({METADATA_KEY_IDENTIFIER: args_bundle["metadata"][METADATA_KEY_IDENTIFIER]})
    bundle_id = instance.save_record(args_bundle["attributes"], args_bundle["metadata"])

    # add relation

    from_record_args = base_connector_record_parameter_example()
    to_record_args = base_connector_record_parameter_example()
    relation_args = base_connector_relation_parameter_example()

    from_label = doc.valid_qualified_name("ex:{}FROM NODE".format(identifier_prefix))
    to_label = doc.valid_qualified_name("ex:{}TO NODE".format(identifier_prefix))
    from_record_args["metadata"][METADATA_KEY_IDENTIFIER] = from_label
    to_record_args["metadata"][METADATA_KEY_IDENTIFIER] = to_label

    from_record_id = instance.save_record(from_record_args["attributes"], from_record_args["metadata"])
    to_record_id = instance.save_record(to_record_args["attributes"], to_record_args["metadata"])


    relation_id = instance.save_relation(from_label, to_label, relation_args["attributes"], relation_args["metadata"])



    return {
        "relation_id": relation_id,
        "from_record_id": from_record_id,
        "to_record_id": to_record_id,
        "bundle_id": bundle_id,
        "doc_record_id": doc_record_id
    }


class AdapterTestTemplate(unittest.TestCase):

    maxDiff = None

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
    def clear_database(self):
        pass
    ### create section ###

    def test_save_bundle(self):
        self.clear_database()
        args = base_connector_bundle_parameter_example()
        id = self.instance.save_record(args["attributes"], args["metadata"])
        self.assertIsNotNone(id)
        self.assertIs(type(id), str, "id should be a string ")

    def test_save_record(self):
        self.clear_database()
        args = base_connector_record_parameter_example()

        record_id  = self.instance.save_record(args["attributes"], args["metadata"])
        self.assertIsNotNone(record_id)
        self.assertIs(type(record_id), str, "id should be a string ")

    def test_save_relation(self):
        self.clear_database()
        args_relation = base_connector_relation_parameter_example()
        args_records = base_connector_record_parameter_example()


        from_meta = args_records["metadata"].copy()
        from_meta.update({METADATA_KEY_IDENTIFIER: args_relation["from_node"]})
        from_node_id  = self.instance.save_record( args_records["attributes"], from_meta)

        to_meta = args_records["metadata"].copy()
        to_meta.update({METADATA_KEY_IDENTIFIER: args_relation["to_node"]})
        to_node_id  = self.instance.save_record(args_records["attributes"], to_meta)


        relation_id = self.instance.save_relation(args_relation["from_node"], args_relation["to_node"], args_relation["attributes"], args_relation["metadata"])
        self.assertIsNotNone(relation_id)
        self.assertIs(type(relation_id), str, "id should be a string ")

    @unittest.skip("We should discuss the possibility of relations that are create automatically nodes")
    def test_save_relation_with_unknown_records(self):
        self.clear_database()
        args_relation = base_connector_relation_parameter_example()


        # Skip the part that creates the from and to node

        relation_id = self.instance.save_relation( args_relation["from_node"],  args_relation["to_node"],
                                                  args_relation["attributes"], args_relation["metadata"])

        self.assertIsNotNone(relation_id)
        self.assertIs(type(relation_id), str, "id should be a string ")

    ### Get section ###


    def test_get_records_by_filter(self):
        self.clear_database()
        args = base_connector_record_parameter_example()
        args_bundle = base_connector_bundle_parameter_example()


        record_id = self.instance.save_record(args["attributes"], args["metadata"])

        raw_result = self.instance.get_records_by_filter()

        # Return structure...
        # raw_doc = {
        #     records: []
        #     identifier: ""
        # }

        # check bundle
        self.assertIsNotNone(raw_result)
        self.assertIsInstance(raw_result, list)
        self.assertEqual(len(raw_result), 1)
        self.assertIsNotNone(raw_result[0].metadata)
        self.assertIsNotNone(raw_result[0].attributes)


        #check if the metadata of the record equals
        attr_dict = encode_dict_values_to_primitive(args["attributes"])
        meta_dict = encode_dict_values_to_primitive(args["metadata"])

        self.assertEqual(raw_result[0].attributes, attr_dict)
        self.assertEqual(raw_result[0].metadata, meta_dict)

        # check bundle

    def test_get_records_by_filter_with_properties(self):
        self.clear_database()
        ids = insert_document_with_bundles(self.instance)


        #test get single node
        bundle_filter = dict()
        bundle_filter.update({PROV_TYPE: "prov:Bundle"})

        raw_bundle_nodes = self.instance.get_records_by_filter(properties_dict=bundle_filter)
        self.assertIsNotNone(raw_bundle_nodes)
        self.assertIsInstance(raw_bundle_nodes,list)
        self.assertEqual(len(raw_bundle_nodes),1)

        self.assertIsNotNone(raw_bundle_nodes[0].attributes)
        self.assertIsNotNone(raw_bundle_nodes[0].metadata)
        self.assertIsNotNone(raw_bundle_nodes[0].attributes[str(PROV_TYPE)] )
        self.assertEqual(raw_bundle_nodes[0].attributes[str(PROV_TYPE)],bundle_filter[PROV_TYPE] )

        #test get rest of the nodes
        #Use the same attributes as the insert_document_with_bundles() method
        node_filter = base_connector_record_parameter_example()["attributes"]

        #remove date value because this is individual for each node
        del node_filter ["ex:date value"]

        raw_nodes = self.instance.get_records_by_filter(properties_dict=node_filter)
        self.assertIsNotNone(raw_nodes)
        self.assertIsInstance(raw_nodes,list)
        self.assertEqual(len(raw_nodes),4)#4 because the relation is also in the restults and 3 nodes, exclude the bundle node

        self.assertIsNotNone(raw_nodes[0].attributes)
        self.assertIsNotNone(raw_nodes[0].metadata)

        #check if the metadata of the record equals

        args = base_connector_record_parameter_example()

        #Remvoe date because it can't be the same
        del args["attributes"]["ex:date value"]
        del raw_nodes[2].attributes["ex:date value"]

        #Delete metdata prov:type because it can be different
        #del args["metadata"]["prov:type"]
        #del raw_nodes[0].metadata["prov:type"]

        attr_dict = encode_dict_values_to_primitive(args["attributes"])
        meta_dict = encode_dict_values_to_primitive(args["metadata"])

        del meta_dict[METADATA_KEY_IDENTIFIER]
        del raw_nodes[2].metadata[METADATA_KEY_IDENTIFIER]

        self.assertEqual(raw_nodes[2].attributes, attr_dict)
        self.assertEqual(raw_nodes[2].metadata, meta_dict)

    def test_get_records_by_filter_with_metadata(self):
        self.clear_database()
        args = base_connector_record_parameter_example()


        record_id = self.instance.save_record(args["attributes"], args["metadata"])

        raw_records = self.instance.get_records_by_filter()


        #Check return raw_records
        self.assertIsNotNone(raw_records )
        self.assertIsInstance(raw_records,list)
        self.assertEqual(len(raw_records),1)
        self.assertIsInstance(raw_records[0].attributes,dict)
        self.assertIsInstance(raw_records[0].metadata,dict)

        #check if the metadata of the record equals
        attr_dict = encode_dict_values_to_primitive(args["attributes"])
        meta_dict = encode_dict_values_to_primitive(args["metadata"])

        self.assertEqual(raw_records[0].attributes, attr_dict)
        self.assertEqual(raw_records[0].metadata, meta_dict)


    def test_get_records_tail(self):
        self.clear_database()
        ids = insert_document_with_bundles(self.instance)

        from_record = self.instance.get_record(ids["from_record_id"])
        to_record = self.instance.get_record(ids["to_record_id"])

        meta_filter = dict()
        meta_filter.update({METADATA_KEY_IDENTIFIER: from_record.metadata[METADATA_KEY_IDENTIFIER]})
        tail_records = self.instance.get_records_tail(metadata_dict=meta_filter)

        self.assertIsNotNone(tail_records)
        self.assertIsInstance(tail_records,list)
        self.assertEqual(len(tail_records),2)#1 node and 1 relation
        self.assertIsNotNone(tail_records[0].attributes)
        self.assertIsNotNone(tail_records[0].metadata)
        self.assertIsInstance(tail_records[0].attributes,dict)
        self.assertIsInstance(tail_records[0].metadata,dict)

        if str(tail_records[0].metadata[METADATA_KEY_IDENTIFIER]) == str(to_record.metadata[METADATA_KEY_IDENTIFIER]):
            db_from_record = tail_records[0]
        elif str(tail_records[1].metadata[METADATA_KEY_IDENTIFIER]) == str(to_record.metadata[METADATA_KEY_IDENTIFIER]):
            db_from_record = tail_records[1]
        else:
            raise Exception("tail_record must contain the to_record identifier")

        tail_records_attr = encode_dict_values_to_primitive(db_from_record.attributes)
        tail_records_meta = encode_dict_values_to_primitive(db_from_record.metadata)
        self.assertEqual(tail_records_attr,to_record.attributes)
        self.assertEqual(tail_records_meta,to_record.metadata)

    def test_get_records_tail_nested(self):
        self.clear_database()
        ids = insert_document_with_bundles(self.instance)
        ids2 = insert_document_with_bundles(self.instance,"second_")

        from_record = self.instance.get_record(ids["from_record_id"])
        to_record = self.instance.get_record(ids["to_record_id"])
        second_from_record = self.instance.get_record(ids2["from_record_id"])
        second_to_record = self.instance.get_record(ids2["to_record_id"])

        relation_params = base_connector_relation_parameter_example()

        self.instance.save_relation(to_record.metadata[METADATA_KEY_IDENTIFIER], second_from_record.metadata[METADATA_KEY_IDENTIFIER],relation_params["attributes"],relation_params["metadata"])
        self.instance.save_relation(second_from_record.metadata[METADATA_KEY_IDENTIFIER], from_record.metadata[METADATA_KEY_IDENTIFIER],relation_params["attributes"],relation_params["metadata"])

        meta_filter = dict()
        meta_filter.update({METADATA_KEY_IDENTIFIER: from_record.metadata[METADATA_KEY_IDENTIFIER]})
        tail_records = self.instance.get_records_tail(metadata_dict=meta_filter)

        self.assertIsNotNone(tail_records)
        self.assertIsInstance(tail_records,list)
        self.assertEqual(len(tail_records),8) #4 Nodes and 4 connections
        self.assertIsNotNone(tail_records[0].attributes)
        self.assertIsNotNone(tail_records[0].metadata)
        self.assertIsInstance(tail_records[0].attributes,dict)
        self.assertIsInstance(tail_records[0].metadata,dict)

    def test_get_bundle_records(self):
        self.clear_database()
        #create relation in database
        doc = ProvDocument()

        from_record_args = base_connector_record_parameter_example()
        to_record_args = base_connector_record_parameter_example()
        relation_args = base_connector_relation_parameter_example()

        from_identifier = relation_args["from_node"]
        to_identifier = relation_args["to_node"]

        #set up bundle
        relation_args["metadata"][METADATA_KEY_PROV_TYPE] = PROV_RECORD_IDS_MAP["wasAssociatedWith"]
        relation_args["attributes"][PROV_TYPE] = doc.valid_qualified_name("prov:bundleAssociation")
        from_record_args["metadata"][METADATA_KEY_IDENTIFIER] = from_identifier
        to_record_args["metadata"][METADATA_KEY_IDENTIFIER] = to_identifier

        from_record_id = self.instance.save_record(from_record_args["attributes"], from_record_args["metadata"])  #
        to_record_id = self.instance.save_record(to_record_args["attributes"], to_record_args["metadata"])  #

        relation_id = self.instance.save_relation(from_identifier, to_identifier, relation_args["attributes"], relation_args["metadata"])

        # add one more
        args = base_connector_record_parameter_example()
        identifier   = doc.valid_qualified_name("prov:anotherNode")
        args["metadata"][METADATA_KEY_IDENTIFIER] =identifier

        relation_args["metadata"][METADATA_KEY_PROV_TYPE] = PROV_RECORD_IDS_MAP["wasAssociatedWith"]

        record_id = self.instance.save_record(args["attributes"], args["metadata"])
        relation_id = self.instance.save_relation(from_identifier, identifier, relation_args["attributes"], relation_args["metadata"])


        relation_args["metadata"][METADATA_KEY_PROV_TYPE] = PROV_RECORD_IDS_MAP["wasGeneratedBy"]
        relation_args["attributes"][PROV_TYPE] = PROV_RECORD_IDS_MAP["wasGeneratedBy"]
        relation_id = self.instance.save_relation(identifier, to_identifier, relation_args["attributes"], relation_args["metadata"])


        #Test the get


        result_records = self.instance.get_bundle_records("ex:Yoda")
        self.assertIsNotNone(result_records)
        self.assertIsInstance(result_records,list)
        self.assertTrue(len(result_records),2)# 2 x node (another node) ,  1 x relation (was informed by )


    def test_get_record(self):
        self.clear_database()
        args = base_connector_record_parameter_example()

        record_id = self.instance.save_record( args["attributes"], args["metadata"])#

        record_raw = self.instance.get_record(record_id)

        self.assertIsNotNone(record_raw)
        self.assertIsNotNone(record_raw.attributes)
        self.assertIsNotNone(record_raw.metadata)
        self.assertIsInstance(record_raw.metadata, dict)
        self.assertIsInstance(record_raw.attributes, dict)

        attributes_primitive = encode_dict_values_to_primitive(args["attributes"])
        metadata_primitive = encode_dict_values_to_primitive(args["metadata"])

        self.assertEqual(record_raw.attributes, attributes_primitive)
        self.assertEqual(record_raw.metadata, metadata_primitive)

        self.assertIs(type(record_id), str, "id should be a string ")

    def test_get_record_not_found(self):
        self.clear_database()
        with self.assertRaises(NotFoundException):
            self.instance.get_record("99999999")

    def test_get_relation(self):
        self.clear_database()
        from_record_args = base_connector_record_parameter_example()
        to_record_args = base_connector_record_parameter_example()
        relation_args = base_connector_relation_parameter_example()

        from_identifier = relation_args["from_node"]
        to_identifier = relation_args["to_node"]
        from_record_args["metadata"][METADATA_KEY_IDENTIFIER] = from_identifier
        to_record_args["metadata"][METADATA_KEY_IDENTIFIER] = to_identifier

        from_record_id = self.instance.save_record(from_record_args["attributes"], from_record_args["metadata"])  #
        to_record_id = self.instance.save_record(to_record_args["attributes"], to_record_args["metadata"])  #

        relation_id = self.instance.save_relation(from_identifier, to_identifier, relation_args["attributes"], relation_args["metadata"])

        relation_raw = self.instance.get_relation(relation_id)

        self.assertIsNotNone(relation_raw)
        self.assertIsNotNone(relation_raw.attributes)
        self.assertIsNotNone(relation_raw.metadata)
        self.assertIsInstance(relation_raw.metadata, dict)
        self.assertIsInstance(relation_raw.attributes, dict)

        attributes_primitive = encode_dict_values_to_primitive(relation_args["attributes"])
        metadata_primitive = encode_dict_values_to_primitive(relation_args["metadata"])

        self.assertEqual(relation_raw.attributes, attributes_primitive)
        self.assertEqual(relation_raw.metadata, metadata_primitive)

    def test_get_relation_not_found(self):
        self.clear_database()
        with self.assertRaises(NotFoundException):
            self.instance.get_relation("99999999")

    ##Delete section ###
    def test_delete_by_filter(self):
        self.clear_database()
        ids = insert_document_with_bundles(self.instance)
        result = self.instance.delete_records_by_filter()
        self.assertIsInstance(result,bool)
        self.assertTrue(result)

        with self.assertRaises(NotFoundException):
            from_record_id  = ids["from_record_id"]
            self.instance.get_record(from_record_id)


        raw_results = self.instance.get_records_by_filter()
        self.assertIsNotNone(raw_results)
        self.assertIsInstance(raw_results,list)
        self.assertEqual(len(raw_results),0)

    def test_delete_by_filter_with_properties(self):
        self.clear_database()
        ids = insert_document_with_bundles(self.instance)

        #Use the same attributes as the insert_document_with_bundles() method
        property_filter= base_connector_record_parameter_example()["attributes"]

        #remove date value because this is individual for each node
        del property_filter["ex:date value"]

        result = self.instance.delete_records_by_filter(property_filter)
        self.assertIsInstance(result,bool)
        self.assertTrue(result)

        with self.assertRaises(NotFoundException):
            from_record_id  = ids["from_record_id"]
            self.instance.get_record(from_record_id)

        with self.assertRaises(NotFoundException):
            to_record_id  = ids["to_record_id"]
            self.instance.get_record(from_record_id)


        #Bundle node should be there because not all attributes are match the filter
        bundle_id = self.instance.get_record(ids["bundle_id"])
        self.assertIsNotNone(bundle_id)

        #After the delete it should be only one node in the database
        raw_results = self.instance.get_records_by_filter(dict())
        self.assertIsNotNone(raw_results)
        self.assertIsInstance(raw_results, list)
        self.assertEqual(len(raw_results), 1)


    def test_delete_by_filter_with_metadata(self):
        self.clear_database()
        ids = insert_document_with_bundles(self.instance)

        from_record_id = ids["from_record_id"]

        from_record = self.instance.get_record(from_record_id)

        # Use the same attributes as the insert_document_with_bundles() method
        property_filter = dict()
        #Get identifier from record and add the identifer to the filter
        property_filter.update({METADATA_KEY_IDENTIFIER: from_record.metadata[METADATA_KEY_IDENTIFIER]})


        #Try to delete only the from node
        result = self.instance.delete_records_by_filter(metadata_dict=property_filter)
        self.assertIsInstance(result, bool)
        self.assertTrue(result)

        with self.assertRaises(NotFoundException):
            from_record_id = ids["from_record_id"]
            self.instance.get_record(from_record_id)

        # After the delete it should be one less in the db
        raw_results = self.instance.get_records_by_filter()
        self.assertIsNotNone(raw_results)
        self.assertIsInstance(raw_results, list)
        self.assertEqual(len(raw_results), 3)

    def test_delete_record(self):
        self.clear_database()
        ids = insert_document_with_bundles(self.instance)
        from_record_id = ids["from_record_id"]
        result = self.instance.delete_record(from_record_id)
        self.assertIsInstance(result, bool)
        self.assertTrue(result)

        with self.assertRaises(NotFoundException):
            self.instance.get_record(from_record_id)

    def test_delete_relation(self):
        self.clear_database()
        ids = insert_document_with_bundles(self.instance)
        relation_id = ids["relation_id"]
        result = self.instance.delete_relation(relation_id)
        self.assertIsInstance(result, bool)
        self.assertTrue(result)

        with self.assertRaises(NotFoundException):
            self.instance.get_relation(relation_id)

    ### Merge documents ###


    @unittest.skip("Not supportet")
    def test_merge_no_merge(self):
        self.clear_database()
        example = base_connector_merge_example()
        #Skip test if this merge mode is not supported


        #set to no_merge
        self.instance.setMergeBehaviour(MergeBehaviour.NO_MERGE)

        #save record test
        self.instance.save_record(example.from_node["attributes"],example.from_node["metadata"])
        with self.assertRaises(MergeException):
            self.instance.save_record(example.from_node["attributes"], example.from_node["metadata"])

        #save relation test
        self.instance.save_record(example.to_node)
        self.instance.save_relation(example.relation)

        with self.assertRaises(MergeException):
            self.instance.save_relation(example.relation)

    def test_merge_record(self):
        self.clear_database()
        example = base_connector_merge_example()
        #Skip test if this merge mode is not supported

        #save record test
        rec_id1 = self.instance.save_record(example.from_node["attributes"],example.from_node["metadata"])
        rec_id2 = self.instance.save_record(example.from_node["attributes"], example.from_node["metadata"])

        self.assertEqual(rec_id1,rec_id2)

        # Test merge result of save record

        db_record = self.instance.get_record(rec_id2) # The id's are equal so it makes no difference

        attr = encode_dict_values_to_primitive(example.from_node["attributes"])
        meta = encode_dict_values_to_primitive(example.from_node["metadata"])
        self.assertEqual(attr,db_record.attributes)

        db_meta = db_record.metadata
        if type(db_record.metadata[METADATA_KEY_NAMESPACES]) is list:
            db_meta = encode_adapter_result_to_excpect(db_record.metadata)

        self.assertEqual(meta,db_meta)
        prim = primer_example()
        self.assertEqual(len(prim.get_records()),len(prim.unified().get_records()))

    def test_merge_record_complex(self):
        self.clear_database()
        example = base_connector_merge_example()
        # Skip test if this merge mode is not supported

        # Save record with different attributes
        rec_id1 = self.instance.save_record(example.from_node["attributes"], example.from_node["metadata"])

        attr_modified = dict()
        attr_modified.update({"ex:a other attribute": True})
        metadata_modified = example.from_node["metadata"].copy()

        rec_id2 = self.instance.save_record(attr_modified, metadata_modified)

        self.assertEqual(rec_id1, rec_id2)

        # Test merge result of save record

        db_record = self.instance.get_record(rec_id2)  # The id's are equal so it makes no difference

        attr = encode_dict_values_to_primitive(example.from_node["attributes"])
        attr.update(attr_modified) # add additional attr to check dict
        meta = encode_dict_values_to_primitive(example.from_node["metadata"])

        self.assertEqual(attr, db_record.attributes)
        self.assertEqual(meta, encode_adapter_result_to_excpect(db_record.metadata))

    def test_merge_record_complex_fail(self):
        self.clear_database()
        example = base_connector_merge_example()
        # Skip test if this merge mode is not supported

        # Save record with different attributes
        rec_id1 = self.instance.save_record(example.from_node["attributes"], example.from_node["metadata"])

        attr_modified = dict()
        attr_modified.update({"ex:int value": 1}) # set int value to 1 instead of 99, should throw exception
        metadata_modified = example.from_node["metadata"].copy()

        #should raise exception because otherwise the attribute would be overridden
        with self.assertRaises(MergeException):
            self.instance.save_record(attr_modified, metadata_modified)

    def test_merge_record_metadata(self):
        self.clear_database()
        example = base_connector_merge_example()
        # Skip test if this merge mode is not supported

        # Save record with different attributes
        rec_id1 = self.instance.save_record(example.from_node["attributes"], example.from_node["metadata"])


        metadata_modified = example.from_node["metadata"].copy()
        metadata_modified.update({METADATA_KEY_TYPE_MAP: {"custom_attr_1": "xds:some_value"}})

        rec_id2 = self.instance.save_record(example.from_node["attributes"], metadata_modified)


        self.assertEqual(rec_id1, rec_id2)

        db_record = self.instance.get_record(rec_id1)
        # try to merge type_map in the database

        attr = encode_dict_values_to_primitive(example.relation["attributes"])
        meta = encode_dict_values_to_primitive(example.relation["metadata"])
        meta_custom = encode_dict_values_to_primitive(metadata_modified)

        # check namespaces
        db_record_namespaces = db_record.metadata[METADATA_KEY_NAMESPACES]
        self.assertIsInstance(db_record_namespaces, list)
        self.assertTrue(len(db_record_namespaces), 2)
        self.assertEqual(meta[METADATA_KEY_NAMESPACES], db_record_namespaces[0])
        self.assertEqual(meta_custom[METADATA_KEY_NAMESPACES], db_record_namespaces[1])

        # check type_map
        db_record_type_map = db_record.metadata[METADATA_KEY_TYPE_MAP]
        self.assertIsInstance(db_record_type_map, list)
        self.assertTrue(len(db_record_type_map), 2)
        self.assertEqual(meta[METADATA_KEY_TYPE_MAP], db_record_type_map[0])
        self.assertEqual(meta_custom[METADATA_KEY_TYPE_MAP], db_record_type_map[1])


    def test_merge_relation(self):
        self.clear_database()
        example = base_connector_merge_example()
        prim = primer_example()
        self.assertEqual(len(prim.get_records()), len(prim.unified().get_records()))
        #save relation test
        self.instance.save_record(example.from_node["attributes"],example.from_node["metadata"])
        self.instance.save_record(example.to_node["attributes"],example.to_node["metadata"])

        from_label = example.from_node["metadata"][METADATA_KEY_IDENTIFIER]
        to_label = example.to_node["metadata"][METADATA_KEY_IDENTIFIER]

        rel_id1 = self.instance.save_relation(from_label,to_label,example.relation["attributes"], example.relation["metadata"])
        rel_id2 = self.instance.save_relation(from_label,to_label,example.relation["attributes"], example.relation["metadata"])

        self.assertEqual(rel_id1,rel_id2)

        # Test merge result of save relation

        db_record = self.instance.get_relation(rel_id2)  # The id's are equal so it makes no difference

        attr = encode_dict_values_to_primitive(example.relation["attributes"])
        meta = encode_dict_values_to_primitive(example.relation["metadata"])

        self.assertEqual(attr, db_record.attributes)
        self.assertEqual(meta, encode_adapter_result_to_excpect(db_record.metadata))


    def test_merge_relation_complex(self):
        self.clear_database()
        example = base_connector_merge_example()
        prim = primer_example()
        self.assertEqual(len(prim.get_records()), len(prim.unified().get_records()))
        #save relation test
        self.instance.save_record(example.from_node["attributes"],example.from_node["metadata"])
        self.instance.save_record(example.to_node["attributes"],example.to_node["metadata"])

        from_label = example.from_node["metadata"][METADATA_KEY_IDENTIFIER]
        to_label = example.to_node["metadata"][METADATA_KEY_IDENTIFIER]

        custom_attributes = dict()
        custom_attributes.update({"My custom value": "value"})

        rel_id1 = self.instance.save_relation(from_label,to_label,example.relation["attributes"], example.relation["metadata"])
        rel_id2 = self.instance.save_relation(from_label,to_label,custom_attributes, example.relation["metadata"])

        self.assertEqual(rel_id1,rel_id2)

        # Test merge result of save relation

        db_record = self.instance.get_relation(rel_id2)  # The id's are equal so it makes no difference

        example.relation["attributes"].update(custom_attributes)
        attr = encode_dict_values_to_primitive(example.relation["attributes"])
        meta = encode_dict_values_to_primitive(example.relation["metadata"])

        self.assertEqual(attr, db_record.attributes)
        self.assertEqual(meta, encode_adapter_result_to_excpect(db_record.metadata))


    def test_merge_relation_complex_fail(self):
        self.clear_database()
        example = base_connector_merge_example()
        prim = primer_example()
        self.assertEqual(len(prim.get_records()), len(prim.unified().get_records()))
        # save relation test
        self.instance.save_record(example.from_node["attributes"], example.from_node["metadata"])
        self.instance.save_record(example.to_node["attributes"], example.to_node["metadata"])

        from_label = example.from_node["metadata"][METADATA_KEY_IDENTIFIER]
        to_label = example.to_node["metadata"][METADATA_KEY_IDENTIFIER]

        #try ot override a propertie in the database
        custom_attributes = dict()
        custom_attributes.update({"ex:int value": 1})

        rel_id1 = self.instance.save_relation(from_label, to_label, example.relation["attributes"],
                                              example.relation["metadata"])

        with self.assertRaises(MergeException):
            rel_id2 = self.instance.save_relation(from_label, to_label, custom_attributes, example.relation["metadata"])


    def test_merge_relation_metadata(self):
        self.clear_database()
        example = base_connector_merge_example()
        prim = primer_example()
        self.assertEqual(len(prim.get_records()), len(prim.unified().get_records()))
        # save relation test
        self.instance.save_record(example.from_node["attributes"], example.from_node["metadata"])
        self.instance.save_record(example.to_node["attributes"], example.to_node["metadata"])

        from_label = example.from_node["metadata"][METADATA_KEY_IDENTIFIER]
        to_label = example.to_node["metadata"][METADATA_KEY_IDENTIFIER]

        # try to merge type_map in the database
        custom_metadata = example.relation["metadata"].copy()
        custom_metadata.update({METADATA_KEY_TYPE_MAP: {"custom_attr_1": "xds:some_value"}})

        rel_id1 = self.instance.save_relation(from_label, to_label, example.relation["attributes"],
                                              example.relation["metadata"])

        rel_id2 = self.instance.save_relation(from_label, to_label, example.relation["attributes"], custom_metadata)


        db_record = self.instance.get_relation(rel_id2)  # The id's are equal so it makes no difference


        attr = encode_dict_values_to_primitive(example.relation["attributes"])
        meta = encode_dict_values_to_primitive(example.relation["metadata"])
        meta_custom = encode_dict_values_to_primitive(custom_metadata)


        self.assertEqual(attr, db_record.attributes)

        #check namespaces
        db_record_namespaces = db_record.metadata[METADATA_KEY_NAMESPACES]
        self.assertIsInstance(db_record_namespaces, list)
        self.assertTrue(len(db_record_namespaces), 2)
        self.assertEqual(meta[METADATA_KEY_NAMESPACES], db_record_namespaces[0])
        self.assertEqual(meta_custom[METADATA_KEY_NAMESPACES], db_record_namespaces[1])

        # check type_map
        db_record_type_map = db_record.metadata[METADATA_KEY_TYPE_MAP]
        self.assertIsInstance(db_record_type_map, list)
        self.assertTrue(len(db_record_type_map), 2)
        self.assertEqual(meta[METADATA_KEY_TYPE_MAP], db_record_type_map[0])
        self.assertEqual(meta_custom[METADATA_KEY_TYPE_MAP], db_record_type_map[1])

class BaseConnectorTests(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_instance_abstract_class(self):
        with self.assertRaises(TypeError):
            instance = BaseAdapter()
