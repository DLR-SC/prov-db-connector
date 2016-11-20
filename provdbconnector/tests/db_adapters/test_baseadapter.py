import json
import unittest

from prov.constants import PROV_TYPE,PROV_RECORD_IDS_MAP
from prov.model import ProvDocument
from provdbconnector.db_adapters.baseadapter import BaseAdapter, METADATA_KEY_IDENTIFIER, METADATA_KEY_TYPE_MAP, METADATA_KEY_NAMESPACES, METADATA_KEY_PROV_TYPE
from provdbconnector.exceptions.database import NotFoundException, MergeException
from provdbconnector.tests.examples import base_connector_record_parameter_example, primer_example,\
    base_connector_relation_parameter_example, base_connector_bundle_parameter_example, base_connector_merge_example
from provdbconnector.utils.serializer import encode_dict_values_to_primitive


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    return repr(obj)

def encode_adapter_result_to_excpect(dict_vals):
    """
    This function translate a metadata dict to an expected version of this dict

    :param dict_vals:
    :return:
    """
    copy = dict_vals.copy()
    if type(copy[METADATA_KEY_NAMESPACES]) is list:
        copy.update({METADATA_KEY_NAMESPACES: copy[METADATA_KEY_NAMESPACES].pop()})

    if type(copy[METADATA_KEY_TYPE_MAP]) is list:
        copy.update({METADATA_KEY_TYPE_MAP: copy[METADATA_KEY_TYPE_MAP].pop()})

    return encode_dict_values_to_primitive(copy)

def insert_document_with_bundles(instance, identifier_prefix=""):
    """
    This function creates a full bundle on your database adapter, to prepare the test data

    :param instance: The db_adapter instnace
    :param identifier_prefix: A prefix for the identifiers

    :return: The ids of the records
    """
    args_record = base_connector_record_parameter_example()
    args_bundle = base_connector_bundle_parameter_example()
    doc = ProvDocument()
    doc.add_namespace("ex", "http://example.com")
    # document with 1 record

    args_record["metadata"].update({METADATA_KEY_IDENTIFIER: doc.valid_qualified_name("ex:" + identifier_prefix + str(args_record["metadata"][METADATA_KEY_IDENTIFIER]))})
    doc_record_id = instance.save_element(args_record["attributes"], args_record["metadata"])

    #bundle with 1 record
    args_bundle["metadata"].update({METADATA_KEY_IDENTIFIER: args_bundle["metadata"][METADATA_KEY_IDENTIFIER]})
    bundle_id = instance.save_element(args_bundle["attributes"], args_bundle["metadata"])

    # add relation

    from_record_args = base_connector_record_parameter_example()
    to_record_args = base_connector_record_parameter_example()
    relation_args = base_connector_relation_parameter_example()

    from_label = doc.valid_qualified_name("ex:{}FROM NODE".format(identifier_prefix))
    to_label = doc.valid_qualified_name("ex:{}TO NODE".format(identifier_prefix))
    from_record_args["metadata"][METADATA_KEY_IDENTIFIER] = from_label
    to_record_args["metadata"][METADATA_KEY_IDENTIFIER] = to_label

    from_record_id = instance.save_element(from_record_args["attributes"], from_record_args["metadata"])
    to_record_id = instance.save_element(to_record_args["attributes"], to_record_args["metadata"])


    relation_id = instance.save_relation(from_label, to_label, relation_args["attributes"], relation_args["metadata"])



    return {
        "relation_id": relation_id,
        "from_record_id": from_record_id,
        "to_record_id": to_record_id,
        "bundle_id": bundle_id,
        "doc_record_id": doc_record_id
    }


class AdapterTestTemplate(unittest.TestCase):
    """
    This test class is a template for each database adapter.
    The following example show how you implement the test for your adapter:

        .. literalinclude:: ../provdbconnector/tests/db_adapters/in_memory/test_simple_in_memory.py
           :linenos:
           :language: python
           :lines: 1-25

    """
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

    def setUp(self):
        """
        Setup the instnace of your database adapter
          .. warning::
            Override this method otherwise all test will fail

        :return:
        """
        # this function will never be executed !!!!
        self.instance = BaseAdapter()

    def clear_database(self):
        """
        This function is to clear your database adapter before each test
        :return:
        """
        pass


    #Create section
    def test_1_save_element(self):
        """
        This test try to save a simple record

        **Graph-Strucutre**

        .. figure:: https://cdn.rawgit.com/dlr-sc/prov-db-connector/master/docs/_images/test_cases/test_1_save_element.svg
           :align: center
           :scale: 50 %

        **Input-Data**

        .. warning::
            This is a json representation of the input data and not the real input data.
            For example metadata.prov_type is a QualifiedName instance
        .. code-block:: json

            {
               "metadata":{
                  "identifier":"<QualifiedName: prov:example_node>",
                  "namespaces":{
                     "ex":"http://example.com",
                     "custom":"http://custom.com"
                  },
                  "prov_type":"<QualifiedName: prov:Activity>",
                  "type_map":{
                     "int value":"int",
                     "date value":"xds:datetime"
                  }
               },
               "attributes":{
                  "ex:individual attribute":"Some value",
                  "ex:date value":"datetime.datetime(2005, 6, 1, 13, 33)",
                  "ex:dict value":{
                     "dict":"value"
                  },
                  "ex:list value":[
                     "list",
                     "of",
                     "strings"
                  ],
                  "ex:double value":99.33,
                  "ex:int value":99
               }
            }

        **Output-Data**

        The output is only a id as string

         `4d3cdc76-467d-4db8-89bf-9accc7b27777`



        """
        self.clear_database()
        args = base_connector_record_parameter_example()

        record_id  = self.instance.save_element(args["attributes"], args["metadata"])
        self.assertIsNotNone(record_id)
        self.assertIs(type(record_id), str, "id should be a string ")

    def test_2_save_relation(self):
        """
        This test try to save a simple relation between 2 identifiers


        **Graph-Strucutre**

        .. figure:: https://cdn.rawgit.com/dlr-sc/prov-db-connector/master/docs/_images/test_cases/test_2_save_relation.svg
           :align: center
           :scale: 50 %

        **Input-Data**

        .. warning::
            This is a json representation of the input data and not the real input data.
            For example metadata.prov_type is a QualifiedName instance

        .. code-block:: json

            {

               "from_node":"<QualifiedName: ex:Yoda>",
               "to_node":"<QualifiedName: ex:Luke Skywalker>",
               "metadata":{
                  "prov_type":"<QualifiedName: prov:Mention>",
                  "type_map":{
                     "date value":"xds:datetime",
                     "int value":"int"
                  },
                  "namespaces":{
                     "custom":"http://custom.com",
                     "ex":"http://example.com"
                  },
                  "identifier":"identifier for the relation"
               },
               "attributes":{
                  "ex:list value":[
                     "list",
                     "of",
                     "strings"
                  ],
                  "ex:int value":99,
                  "ex:double value":99.33,
                  "ex:individual attribute":"Some value",
                  "ex:dict value":{
                     "dict":"value"
                  },
                  "ex:date value":"datetime.datetime(2005, 6, 1, 13, 33)"
               },
            }

        **Output-Data**

        The output is only the id of the relation as string

         `4d3cdc76-467d-4db8-89bf-9accc7b27778`



        """

        self.clear_database()
        args_relation = base_connector_relation_parameter_example()
        args_records = base_connector_record_parameter_example()


        from_meta = args_records["metadata"].copy()
        from_meta.update({METADATA_KEY_IDENTIFIER: args_relation["from_node"]})
        self.instance.save_element(args_records["attributes"], from_meta)

        to_meta = args_records["metadata"].copy()
        to_meta.update({METADATA_KEY_IDENTIFIER: args_relation["to_node"]})
        self.instance.save_element(args_records["attributes"], to_meta)


        relation_id = self.instance.save_relation(args_relation["from_node"], args_relation["to_node"], args_relation["attributes"], args_relation["metadata"])
        self.assertIsNotNone(relation_id)
        self.assertIs(type(relation_id), str, "id should be a string ")

    @unittest.skip("We should discuss the possibility of relations that are create automatically nodes")
    def test_3_save_relation_with_unknown_records(self):
        """
        This test is to test the creation of a relation where the nodes are not in the database
        :return:
        """
        self.clear_database()
        args_relation = base_connector_relation_parameter_example()


        # Skip the part that creates the from and to node

        relation_id = self.instance.save_relation( args_relation["from_node"],  args_relation["to_node"],
                                                  args_relation["attributes"], args_relation["metadata"])

        self.assertIsNotNone(relation_id)
        self.assertIs(type(relation_id), str, "id should be a string ")

    def test_4_get_record(self):
        """
        Create a record and then try to get it back

        **Graph-Strucutre**

        .. figure:: https://cdn.rawgit.com/dlr-sc/prov-db-connector/master/docs/_images/test_cases/test_4_get_record.svg
           :align: center
           :scale: 50 %

        **Input-Data**

        `"id-333"`

        **Output-Data**

        The output is all connected nodes and there relations

        .. code-block:: json

            [
               {
                  "ex:int value":99,
                  "ex:list value":[
                     "list",
                     "of",
                     "strings"
                  ],
                  "ex:date value":"2005-06-01 13:33:00",
                  "ex:individual attribute":"Some value",
                  "ex:double value":99.33,
                  "ex:dict value":"{\"dict\": \"value\"}"
               },
               {
                  "identifier":"prov:example_node",
                  "prov_type":"prov:Activity",
                  "type_map":"{\"date value\": \"xds:datetime\", \"int value\": \"int\"}",
                  "namespaces":"{\"custom\": \"http://custom.com\", \"ex\": \"http://example.com\"}"
               }
            ]

        """
        self.clear_database()
        args = base_connector_record_parameter_example()

        record_id = self.instance.save_element(args["attributes"], args["metadata"])#

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

    def test_5_get_record_not_found(self):
        """
        Try to get a record with a invalid id
        :return:
        """
        self.clear_database()
        with self.assertRaises(NotFoundException):
            self.instance.get_record("99999999")

    def test_6_get_relation(self):
        """
        create a relation between 2 nodes and try to get the relation back


        **Graph-Strucutre**

        .. figure:: https://cdn.rawgit.com/dlr-sc/prov-db-connector/master/docs/_images/test_cases/test_6_get_relation.svg
           :align: center
           :scale: 50 %


        **Input-Data**

        `"id-333"`

        **Output-Data**

        The output is all connected nodes and there relations

        .. code-block:: json

            [
               {
                  "ex:dict value":"{\"dict\": \"value\"}",
                  "ex:int value":99,
                  "ex:individual attribute":"Some value",
                  "ex:date value":"2005-06-01 13:33:00",
                  "ex:list value":[
                     "list",
                     "of",
                     "strings"
                  ],
                  "ex:double value":99.33
               },
               {
                  "identifier":"identifier for the relation",
                  "prov_type":"prov:Mention",
                  "namespaces":"{\"ex\": \"http://example.com\", \"custom\": \"http://custom.com\"}",
                  "type_map":"{\"date value\": \"xds:datetime\", \"int value\": \"int\"}"
               }
            ]

        """
        self.clear_database()
        from_record_args = base_connector_record_parameter_example()
        to_record_args = base_connector_record_parameter_example()
        relation_args = base_connector_relation_parameter_example()

        from_identifier = relation_args["from_node"]
        to_identifier = relation_args["to_node"]
        from_record_args["metadata"][METADATA_KEY_IDENTIFIER] = from_identifier
        to_record_args["metadata"][METADATA_KEY_IDENTIFIER] = to_identifier

        from_record_id = self.instance.save_element(from_record_args["attributes"], from_record_args["metadata"])  #
        to_record_id = self.instance.save_element(to_record_args["attributes"], to_record_args["metadata"])  #

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

    def test_7_get_relation_not_found(self):
        """
        Try to get a not existing relation


        """
        self.clear_database()
        with self.assertRaises(NotFoundException):
            self.instance.get_relation("99999999")

    ##Filter-Section##
    def test_8_get_records_by_filter(self):
        """

        This test is to get a the whole graph without any filter

        **Graph-Strucutre**

        .. figure:: https://cdn.rawgit.com/dlr-sc/prov-db-connector/master/docs/_images/test_cases/test_8_get_records_by_filter.svg
           :align: center
           :scale: 50 %


        **Input-Data**

        We have no input data for the filter function because we want to get the whole graph

        **Output-Data**

        The output is the only node that exist in the database (was also created during the test)

        .. warning::
            This is a json representation of the input data and not the real input data.
            For example metadata.prov_type is a QualifiedName instance
        .. code-block:: json

            {
               "metadata":{
                  "identifier":"<QualifiedName: prov:example_node>",
                  "namespaces":{
                     "ex":"http://example.com",
                     "custom":"http://custom.com"
                  },
                  "prov_type":"<QualifiedName: prov:Activity>",
                  "type_map":{
                     "int value":"int",
                     "date value":"xds:datetime"
                  }
               },
               "attributes":{
                  "ex:individual attribute":"Some value",
                  "ex:date value":"datetime.datetime(2005, 6, 1, 13, 33)",
                  "ex:dict value":{
                     "dict":"value"
                  },
                  "ex:list value":[
                     "list",
                     "of",
                     "strings"
                  ],
                  "ex:double value":99.33,
                  "ex:int value":99
               }
            }

        """
        self.clear_database()
        args = base_connector_record_parameter_example()
        base_connector_bundle_parameter_example()

        record_id = self.instance.save_element(args["attributes"], args["metadata"])

        raw_result = self.instance.get_records_by_filter()

        # check restul
        self.assertIsNotNone(raw_result)
        self.assertIsInstance(raw_result, list)
        self.assertEqual(len(raw_result), 1)
        self.assertIsNotNone(raw_result[0].metadata)
        self.assertIsNotNone(raw_result[0].attributes)

        # check if the metadata of the record equals
        attr_dict = encode_dict_values_to_primitive(args["attributes"])
        meta_dict = encode_dict_values_to_primitive(args["metadata"])

        self.assertEqual(raw_result[0].attributes, attr_dict)
        self.assertEqual(raw_result[0].metadata, meta_dict)

        # check bundle

    def test_9_get_records_by_filter_with_properties(self):
        """
        This test is to get a specific part of the graph via certain filter criteria


        **Graph-Strucutre**

        .. figure:: https://cdn.rawgit.com/dlr-sc/prov-db-connector/master/docs/_images/test_cases/test_9_get_records_by_filter_with_properties.svg
           :align: center
           :scale: 50 %

        *Get single node*

        The first part of the test is to try to get a single node based on a attribut

        *Input-Data*

        .. code-block:: json

            {
                "prov:type":"prov:Bundle"
            }


        *Output-Data*

        The output is a list of namedtuples wit the following structure: `list(tuple(attributes,metadata))`

        .. code-block:: json

            [
                [
                    {
                        "prov:type": "prov:Bundle"
                    },
                    {
                        "prov_type": "prov:Entity",
                        "identifier": "ex:bundle name",
                        "type_map": "{\\"date value\\": \\"xds:datetime\\", \\"int value\\": \\"int\\"}",
                        "namespaces": "{\\"ex\\": \\"http://example.com\\"}"
                    }
                ]
            ]


        *Get other nodes*

        The second part tests the other way to get all other node except the bundle node

        *Input-Data*

        The input is a set of attributes

        .. code-block:: json

            {
               "ex:dict value":{
                  "dict":"value"
               },
               "ex:double value":99.33,
               "ex:int value":99,
               "ex:individual attribute":"Some value",
               "ex:list value":[
                  "list",
                  "of",
                  "strings"
               ]
            }

        *Output-Data*

        The output is a list of namedtuple with attributes and metadata

        .. code-block:: json

            [
               [
                  {
                     "ex:dict value":"{'dict': 'value'}",
                     "ex:double value":99.33,
                     "ex:list value":[
                        "list",
                        "of",
                        "strings"
                     ],
                     "ex:int value":99,
                     "ex:date value":"2005-06-01 13:33:00",
                     "ex:individual attribute":"Some value"
                  },
                  {
                     "identifier":"ex:TO NODE",
                     "type_map":"{'int value': 'int', 'date value': 'xds:datetime'}",
                     "namespaces":"{'ex': 'http://example.com', 'custom': 'http://custom.com'}",
                     "prov_type":"prov:Activity"
                  }
               ],
               [
                  {
                     "ex:dict value":"{'dict': 'value'}",
                     "ex:double value":99.33,
                     "ex:list value":[
                        "list",
                        "of",
                        "strings"
                     ],
                     "ex:int value":99,
                     "ex:date value":"2005-06-01 13:33:00",
                     "ex:individual attribute":"Some value"
                  },
                  {
                     "identifier":"ex:FROM NODE",
                     "type_map":"{'int value': 'int', 'date value': 'xds:datetime'}",
                     "namespaces":"{'ex': 'http://example.com', 'custom': 'http://custom.com'}",
                     "prov_type":"prov:Activity"
                  }
               ],
               [
                  {
                     "ex:dict value":"{'dict': 'value'}",
                     "ex:double value":99.33,
                     "ex:list value":[
                        "list",
                        "of",
                        "strings"
                     ],
                     "ex:int value":99,
                     "ex:date value":"2005-06-01 13:33:00",
                     "ex:individual attribute":"Some value"
                  },
                  {
                     "identifier":"ex:prov:example_node",
                     "type_map":"{'int value': 'int', 'date value': 'xds:datetime'}",
                     "namespaces":"{'ex': 'http://example.com', 'custom': 'http://custom.com'}",
                     "prov_type":"prov:Activity"
                  }
               ],
               [
                  {
                     "ex:dict value":"{'dict': 'value'}",
                     "ex:double value":99.33,
                     "ex:list value":[
                        "list",
                        "of",
                        "strings"
                     ],
                     "ex:int value":99,
                     "ex:date value":"2005-06-01 13:33:00",
                     "ex:individual attribute":"Some value"
                  },
                  {
                     "identifier":"identifier for the relation",
                     "type_map":"{'int value': 'int', 'date value': 'xds:datetime'}",
                     "namespaces":"{'ex': 'http://example.com', 'custom': 'http://custom.com'}",
                     "prov_type":"prov:Mention"
                  }
               ]
            ]

        :return

        """
        self.clear_database()
        ids = insert_document_with_bundles(self.instance)

        # test get single node
        bundle_filter = dict()
        bundle_filter.update({PROV_TYPE: "prov:Bundle"})

        raw_bundle_nodes = self.instance.get_records_by_filter(attributes_dict=bundle_filter)

        self.assertIsNotNone(raw_bundle_nodes)
        self.assertIsInstance(raw_bundle_nodes, list)
        self.assertEqual(len(raw_bundle_nodes), 1)

        self.assertIsNotNone(raw_bundle_nodes[0].attributes)
        self.assertIsNotNone(raw_bundle_nodes[0].metadata)
        self.assertIsNotNone(raw_bundle_nodes[0].attributes[str(PROV_TYPE)])
        self.assertEqual(raw_bundle_nodes[0].attributes[str(PROV_TYPE)], bundle_filter[PROV_TYPE])

        # test get rest of the nodes
        # Use the same attributes as the insert_document_with_bundles() method
        node_filter = base_connector_record_parameter_example()["attributes"]

        # remove date value because this is individual for each node
        del node_filter["ex:date value"]

        raw_nodes = self.instance.get_records_by_filter(attributes_dict=node_filter)
        self.assertIsNotNone(raw_nodes)
        self.assertIsInstance(raw_nodes, list)
        self.assertEqual(len(raw_nodes),
                         4)  # 4 because the relation is also in the restults and 3 nodes, exclude the bundle node

        self.assertIsNotNone(raw_nodes[0].attributes)
        self.assertIsNotNone(raw_nodes[0].metadata)

        # check if the metadata of the record equals

        args = base_connector_record_parameter_example()

        # Remvoe date because it can't be the same
        del args["attributes"]["ex:date value"]
        del raw_nodes[2].attributes["ex:date value"]

        # Delete metdata prov:type because it can be different
        # del args["metadata"]["prov:type"]
        # del raw_nodes[0].metadata["prov:type"]

        attr_dict = encode_dict_values_to_primitive(args["attributes"])
        meta_dict = encode_dict_values_to_primitive(args["metadata"])

        del meta_dict[METADATA_KEY_IDENTIFIER]
        del raw_nodes[2].metadata[METADATA_KEY_IDENTIFIER]

        self.assertEqual(raw_nodes[2].attributes, attr_dict)
        self.assertEqual(raw_nodes[2].metadata, meta_dict)

    def test_10_get_records_by_filter_with_metadata(self):
        """
        Should test also the filter by metadata

        @todo implement test for filter by metadata


        **Graph-Strucutre**

        .. figure:: https://cdn.rawgit.com/dlr-sc/prov-db-connector/master/docs/_images/test_cases/test_10_get_records_by_filter_with_metadata.svg
           :align: center
           :scale: 50 %


        .. warning::
            This test is not implemented jet

        :return:
        """
        self.clear_database()
        args = base_connector_record_parameter_example()

        record_id = self.instance.save_element(args["attributes"], args["metadata"])

        raw_records = self.instance.get_records_by_filter()

        # Check return raw_records
        self.assertIsNotNone(raw_records)
        self.assertIsInstance(raw_records, list)
        self.assertEqual(len(raw_records), 1)
        self.assertIsInstance(raw_records[0].attributes, dict)
        self.assertIsInstance(raw_records[0].metadata, dict)

        # check if the metadata of the record equals
        attr_dict = encode_dict_values_to_primitive(args["attributes"])
        meta_dict = encode_dict_values_to_primitive(args["metadata"])

        self.assertEqual(raw_records[0].attributes, attr_dict)
        self.assertEqual(raw_records[0].metadata, meta_dict)

    def test_11_get_records_tail(self):
        """
        This test is to get the whole provenance from a starting point

        **Graph-Strucutre**

        .. figure:: https://cdn.rawgit.com/dlr-sc/prov-db-connector/master/docs/_images/test_cases/test_11_get_records_tail.svg
           :align: center
           :scale: 50 %

        **Input-Data**

        In this case we filter by metadata and by the identifier

        .. code-block:: json

            {
               "identifier":"ex:FROM NODE"
            }

        **Output-Data**

        The output is all connected nodes and there relations

        .. code-block:: json

            [
               [
                  {
                     "ex:list value":[
                        "list",
                        "of",
                        "strings"
                     ],
                     "ex:double value":99.33,
                     "ex:int value":99,
                     "ex:individual attribute":"Some value",
                     "ex:date value":"datetime.datetime(2005, 6, 1, 13, 33)",
                     "ex:dict value":{
                        "dict":"value"
                     }
                  },
                  {
                     "prov_type":"<QualifiedName: prov:Mention>",
                     "identifier":"identifier for the relation",
                     "type_map":{
                        "date value":"xds:datetime",
                        "int value":"int"
                     },
                     "namespaces":{
                        "custom":"http://custom.com",
                        "ex":"http://example.com"
                     }
                  }
               ],
               [
                  {
                     "ex:list value":[
                        "list",
                        "of",
                        "strings"
                     ],
                     "ex:double value":99.33,
                     "ex:int value":99,
                     "ex:individual attribute":"Some value",
                     "ex:date value":"datetime.datetime(2005, 6, 1, 13, 33)",
                     "ex:dict value":{
                        "dict":"value"
                     }
                  },
                  {
                     "prov_type":"<QualifiedName: prov:Activity>",
                     "identifier":"<QualifiedName: ex:TO NODE>",
                     "type_map":{
                        "date value":"xds:datetime",
                        "int value":"int"
                     },
                     "namespaces":{
                        "custom":"http://custom.com",
                        "ex":"http://example.com"
                     }
                  }
               ]
            ]

        """
        self.clear_database()
        ids = insert_document_with_bundles(self.instance)

        from_record = self.instance.get_record(ids["from_record_id"])
        to_record = self.instance.get_record(ids["to_record_id"])

        meta_filter = dict()
        meta_filter.update({METADATA_KEY_IDENTIFIER: from_record.metadata[METADATA_KEY_IDENTIFIER]})
        tail_records = self.instance.get_records_tail(metadata_dict=meta_filter)

        self.assertIsNotNone(tail_records)
        self.assertIsInstance(tail_records, list)
        self.assertEqual(len(tail_records), 2)  # 1 node and 1 relation
        self.assertIsNotNone(tail_records[0].attributes)
        self.assertIsNotNone(tail_records[0].metadata)
        self.assertIsInstance(tail_records[0].attributes, dict)
        self.assertIsInstance(tail_records[0].metadata, dict)

        if str(tail_records[0].metadata[METADATA_KEY_IDENTIFIER]) == str(
                to_record.metadata[METADATA_KEY_IDENTIFIER]):
            db_from_record = tail_records[0]
        elif str(tail_records[1].metadata[METADATA_KEY_IDENTIFIER]) == str(
                to_record.metadata[METADATA_KEY_IDENTIFIER]):
            db_from_record = tail_records[1]
        else:
            raise Exception("tail_record must contain the to_record identifier")

        tail_records_attr = encode_dict_values_to_primitive(db_from_record.attributes)
        tail_records_meta = encode_dict_values_to_primitive(db_from_record.metadata)
        self.assertEqual(tail_records_attr, to_record.attributes)
        self.assertEqual(tail_records_meta, to_record.metadata)

    def test_12_get_records_tail_recursive(self):
        """
        Test the same behavior as the `test_get_records_tail` test but with a recursive data structure

        **Graph-Strucutre**

        .. figure:: https://cdn.rawgit.com/dlr-sc/prov-db-connector/master/docs/_images/test_cases/test_12_get_records_tail_recursive.svg
           :align: center
           :scale: 50 %

        **Input-Data**

        .. code-block:: json

            {
               "identifier":"ex:FROM NODE"
            }

        **Output-Data**

        The output is all connected nodes and there relations

        .. code-block:: json

            [
               [
                  {
                     "ex:dict value":{
                        "dict":"value"
                     },
                     "ex:date value":"datetime.datetime(2005, 6, 1, 13, 33)",
                     "ex:double value":99.33,
                     "ex:individual attribute":"Some value",
                     "ex:int value":99,
                     "ex:list value":[
                        "list",
                        "of",
                        "strings"
                     ]
                  },
                  {
                     "namespaces":{
                        "custom":"http://custom.com",
                        "ex":"http://example.com"
                     },
                     "identifier":"identifier for the relation",
                     "prov_type":"<QualifiedName: prov:Mention>",
                     "type_map":{
                        "date value":"xds:datetime",
                        "int value":"int"
                     }
                  }
               ],
               [
                  {
                     "ex:dict value":{
                        "dict":"value"
                     },
                     "ex:date value":"datetime.datetime(2005, 6, 1, 13, 33)",
                     "ex:double value":99.33,
                     "ex:individual attribute":"Some value",
                     "ex:int value":99,
                     "ex:list value":[
                        "list",
                        "of",
                        "strings"
                     ]
                  },
                  {
                     "namespaces":{
                        "custom":"http://custom.com",
                        "ex":"http://example.com"
                     },
                     "identifier":"identifier for the relation",
                     "prov_type":"<QualifiedName: prov:Mention>",
                     "type_map":{
                        "date value":"xds:datetime",
                        "int value":"int"
                     }
                  }
               ],
               [
                  {
                     "ex:dict value":{
                        "dict":"value"
                     },
                     "ex:date value":"datetime.datetime(2005, 6, 1, 13, 33)",
                     "ex:double value":99.33,
                     "ex:individual attribute":"Some value",
                     "ex:int value":99,
                     "ex:list value":[
                        "list",
                        "of",
                        "strings"
                     ]
                  },
                  {
                     "namespaces":{
                        "custom":"http://custom.com",
                        "ex":"http://example.com"
                     },
                     "identifier":"<QualifiedName: ex:TO NODE>",
                     "prov_type":"<QualifiedName: prov:Activity>",
                     "type_map":{
                        "date value":"xds:datetime",
                        "int value":"int"
                     }
                  }
               ],
               [
                  {
                     "ex:dict value":{
                        "dict":"value"
                     },
                     "ex:date value":"datetime.datetime(2005, 6, 1, 13, 33)",
                     "ex:double value":99.33,
                     "ex:individual attribute":"Some value",
                     "ex:int value":99,
                     "ex:list value":[
                        "list",
                        "of",
                        "strings"
                     ]
                  },
                  {
                     "namespaces":{
                        "custom":"http://custom.com",
                        "ex":"http://example.com"
                     },
                     "identifier":"<QualifiedName: ex:second_TO NODE>",
                     "prov_type":"<QualifiedName: prov:Activity>",
                     "type_map":{
                        "date value":"xds:datetime",
                        "int value":"int"
                     }
                  }
               ],
               [
                  {
                     "ex:dict value":{
                        "dict":"value"
                     },
                     "ex:date value":"datetime.datetime(2005, 6, 1, 13, 33)",
                     "ex:double value":99.33,
                     "ex:individual attribute":"Some value",
                     "ex:int value":99,
                     "ex:list value":[
                        "list",
                        "of",
                        "strings"
                     ]
                  },
                  {
                     "namespaces":{
                        "custom":"http://custom.com",
                        "ex":"http://example.com"
                     },
                     "identifier":"identifier for the relation",
                     "prov_type":"<QualifiedName: prov:Mention>",
                     "type_map":{
                        "date value":"xds:datetime",
                        "int value":"int"
                     }
                  }
               ],
               [
                  {
                     "ex:dict value":{
                        "dict":"value"
                     },
                     "ex:date value":"datetime.datetime(2005, 6, 1, 13, 33)",
                     "ex:double value":99.33,
                     "ex:individual attribute":"Some value",
                     "ex:int value":99,
                     "ex:list value":[
                        "list",
                        "of",
                        "strings"
                     ]
                  },
                  {
                     "namespaces":{
                        "custom":"http://custom.com",
                        "ex":"http://example.com"
                     },
                     "identifier":"<QualifiedName: ex:FROM NODE>",
                     "prov_type":"<QualifiedName: prov:Activity>",
                     "type_map":{
                        "date value":"xds:datetime",
                        "int value":"int"
                     }
                  }
               ],
               [
                  {
                     "ex:dict value":{
                        "dict":"value"
                     },
                     "ex:date value":"datetime.datetime(2005, 6, 1, 13, 33)",
                     "ex:double value":99.33,
                     "ex:individual attribute":"Some value",
                     "ex:int value":99,
                     "ex:list value":[
                        "list",
                        "of",
                        "strings"
                     ]
                  },
                  {
                     "namespaces":{
                        "custom":"http://custom.com",
                        "ex":"http://example.com"
                     },
                     "identifier":"identifier for the relation",
                     "prov_type":"<QualifiedName: prov:Mention>",
                     "type_map":{
                        "date value":"xds:datetime",
                        "int value":"int"
                     }
                  }
               ],
               [
                  {
                     "ex:dict value":{
                        "dict":"value"
                     },
                     "ex:date value":"datetime.datetime(2005, 6, 1, 13, 33)",
                     "ex:double value":99.33,
                     "ex:individual attribute":"Some value",
                     "ex:int value":99,
                     "ex:list value":[
                        "list",
                        "of",
                        "strings"
                     ]
                  },
                  {
                     "namespaces":{
                        "custom":"http://custom.com",
                        "ex":"http://example.com"
                     },
                     "identifier":"<QualifiedName: ex:second_FROM NODE>",
                     "prov_type":"<QualifiedName: prov:Activity>",
                     "type_map":{
                        "date value":"xds:datetime",
                        "int value":"int"
                     }
                  }
               ]
            ]



        """
        # def test_get_records_tail_recursive(self):

        self.clear_database()
        ids = insert_document_with_bundles(self.instance)
        ids2 = insert_document_with_bundles(self.instance, "second_")

        from_record = self.instance.get_record(ids["from_record_id"])
        to_record = self.instance.get_record(ids["to_record_id"])
        second_from_record = self.instance.get_record(ids2["from_record_id"])
        second_to_record = self.instance.get_record(ids2["to_record_id"])

        relation_params = base_connector_relation_parameter_example()

        self.instance.save_relation(to_record.metadata[METADATA_KEY_IDENTIFIER],
                                    second_from_record.metadata[METADATA_KEY_IDENTIFIER],
                                    relation_params["attributes"], relation_params["metadata"])
        self.instance.save_relation(second_from_record.metadata[METADATA_KEY_IDENTIFIER],
                                    from_record.metadata[METADATA_KEY_IDENTIFIER],
                                    relation_params["attributes"], relation_params["metadata"])

        meta_filter = dict()
        meta_filter.update({METADATA_KEY_IDENTIFIER: from_record.metadata[METADATA_KEY_IDENTIFIER]})
        tail_records = self.instance.get_records_tail(metadata_dict=meta_filter)

        self.assertIsNotNone(tail_records)
        self.assertIsInstance(tail_records, list)
        self.assertEqual(len(tail_records), 8)  # 4 Nodes and 4 connections
        self.assertIsNotNone(tail_records[0].attributes)
        self.assertIsNotNone(tail_records[0].metadata)
        self.assertIsInstance(tail_records[0].attributes, dict)
        self.assertIsInstance(tail_records[0].metadata, dict)

    def test_13_get_bundle_records(self):
        """
        The get_bundle function is to return the records (relation and nodes) for a bundle identifier

        Test the same behavior as the `test_get_records_tail` test but with a recursive data structure

        **Graph-Strucutre**

        .. figure:: https://cdn.rawgit.com/dlr-sc/prov-db-connector/master/docs/_images/test_cases/test_13_get_bundle_records.svg
           :align: center
           :scale: 50 %

        **Input-Data**

        .. code-block:: json

            {
               "identifier":"ex:FROM NODE"
            }

        **Output-Data**

        The output is all connected nodes and there relations

         .. warning::
            coming soon!


        """
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

        from_record_id = self.instance.save_element(from_record_args["attributes"], from_record_args["metadata"])  #
        to_record_id = self.instance.save_element(to_record_args["attributes"], to_record_args["metadata"])  #

        relation_id = self.instance.save_relation(to_identifier, from_identifier, relation_args["attributes"], relation_args["metadata"])

        # add one more
        args = base_connector_record_parameter_example()
        identifier   = doc.valid_qualified_name("prov:anotherNode")
        args["metadata"][METADATA_KEY_IDENTIFIER] =identifier

        relation_args["metadata"][METADATA_KEY_PROV_TYPE] = PROV_RECORD_IDS_MAP["wasAssociatedWith"]

        record_id = self.instance.save_element(args["attributes"], args["metadata"])
        relation_id = self.instance.save_relation(identifier, from_identifier, relation_args["attributes"], relation_args["metadata"])


        relation_args["metadata"][METADATA_KEY_PROV_TYPE] = PROV_RECORD_IDS_MAP["wasGeneratedBy"]
        relation_args["attributes"][PROV_TYPE] = PROV_RECORD_IDS_MAP["wasGeneratedBy"]
        relation_id = self.instance.save_relation(identifier, to_identifier, relation_args["attributes"], relation_args["metadata"])


        #Test the get
        result_records = self.instance.get_bundle_records("ex:Yoda")

        self.assertIsNotNone(result_records)
        self.assertIsInstance(result_records,list)
        self.assertTrue(len(result_records),2)# 2 x node (another node) ,  1 x relation (was informed by )

    ##Delete section ###
    def test_14_delete_by_filter(self):
        """
        Try to all records of the database

        :return:
        """
        self.clear_database()
        ids = insert_document_with_bundles(self.instance)
        result = self.instance.delete_records_by_filter()
        self.assertIsInstance(result,bool)
        self.assertTrue(result)

        with self.assertRaises(NotFoundException):
            from_record_id  = ids["from_record_id"]
            self.instance.get_record(from_record_id)


    def test_15_delete_by_filter_with_properties(self):
        """
        Try to delete by filter, same behavior as get_by_filter

        :return:
        """
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
            self.instance.get_record(to_record_id)


        #Bundle node should be there because not all attributes are match the filter
        bundle_id = self.instance.get_record(ids["bundle_id"])
        self.assertIsNotNone(bundle_id)

        #After the delete it should be only one node in the database
        raw_results = self.instance.get_records_by_filter(dict())
        self.assertIsNotNone(raw_results)
        self.assertIsInstance(raw_results, list)
        self.assertEqual(len(raw_results), 1)  #one relation and one node


    def test_16_delete_by_filter_with_metadata(self):
        """
        Try to delete by metadata, same behavior as get_by_metadata
        :return:
        """
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

    def test_17_delete_record(self):
        """
        Delete a single record based on the id

        :return:
        """
        self.clear_database()
        ids = insert_document_with_bundles(self.instance)
        from_record_id = ids["from_record_id"]
        result = self.instance.delete_record(from_record_id)
        self.assertIsInstance(result, bool)
        self.assertTrue(result)

        with self.assertRaises(NotFoundException):
            self.instance.get_record(from_record_id)

    def test_18_delete_relation(self):
        """
        Delete a singe relation based on the relation id

        :return:
        """
        self.clear_database()
        ids = insert_document_with_bundles(self.instance)
        relation_id = ids["relation_id"]
        result = self.instance.delete_relation(relation_id)
        self.assertIsInstance(result, bool)
        self.assertTrue(result)

        with self.assertRaises(NotFoundException):
            self.instance.get_relation(relation_id)

    ### Merge documents ###

    def test_19_merge_record(self):
        """
        This function test the merge abbility of your adapter.


        **Graph-Strucutre**

        .. figure:: https://cdn.rawgit.com/dlr-sc/prov-db-connector/master/docs/_images/test_cases/test_19_merge_record.svg
           :align: center
           :scale: 50 %

        **Input-Data**

        We try to create the node twice, with the following data

        .. code-block:: json

            {
               "metadata":{
                  "prov_type":"<QualifiedName: prov:Activity>",
                  "namespaces":{
                     "ex":"http://example.com",
                     "custom":"http://custom.com"
                  },
                  "identifier":"<QualifiedName: ex:Yoda>",
                  "type_map":{
                     "int value":"int",
                     "date value":"xds:datetime"
                  }
               },
               "attributes":{
                  "ex:int value":99,
                  "ex:individual attribute":"Some value",
                  "ex:list value":[
                     "list",
                     "of",
                     "strings"
                  ],
                  "ex:date value":"datetime.datetime(2005, 6, 1, 13, 33)",
                  "ex:dict value":{
                     "dict":"value"
                  },
                  "ex:double value":99.33
               }
            }

        **Output-Data**

        The output is one entry with no change of the data

        .. code-block:: json

            [
               {
                  "ex:int value":99,
                  "ex:individual attribute":"Some value",
                  "ex:list value":[
                     "list",
                     "of",
                     "strings"
                  ],
                  "ex:date value":"2005-06-01 13:33:00",
                  "ex:dict value":"{\"dict\": \"value\"}",
                  "ex:double value":99.33
               },
               {
                  "prov_type":"prov:Activity",
                  "namespaces":"{\"ex\": \"http://example.com\", \"custom\": \"http://custom.com\"}",
                  "identifier":"ex:Yoda",
                  "type_map":"{\"int value\": \"int\", \"date value\": \"xds:datetime\"}"
               }
            ]


        """
        self.clear_database()
        example = base_connector_merge_example()
        #Skip test if this merge mode is not supported

        #save record test
        rec_id1 = self.instance.save_element(example.from_node["attributes"], example.from_node["metadata"])
        rec_id2 = self.instance.save_element(example.from_node["attributes"], example.from_node["metadata"])

        self.assertEqual(rec_id1,rec_id2)

        # Test merge result of save record

        db_record = self.instance.get_record(rec_id2) # The id's are equal so it makes no difference

        attr = encode_dict_values_to_primitive(example.from_node["attributes"])
        meta = encode_dict_values_to_primitive(example.from_node["metadata"])


        db_meta = encode_adapter_result_to_excpect(db_record.metadata).copy()
        #transform string into dict to compare result

        meta.update({METADATA_KEY_NAMESPACES: json.loads(meta[METADATA_KEY_NAMESPACES])})
        meta.update({METADATA_KEY_TYPE_MAP: json.loads(meta[METADATA_KEY_TYPE_MAP])})

        db_meta.update({METADATA_KEY_NAMESPACES: json.loads(db_meta[METADATA_KEY_NAMESPACES])})
        db_meta.update({METADATA_KEY_TYPE_MAP: json.loads(db_meta[METADATA_KEY_TYPE_MAP])})
        self.assertEqual(attr,db_record.attributes)


        self.assertEqual(meta,db_meta)
        prim = primer_example()
        self.assertEqual(len(prim.get_records()),len(prim.unified().get_records()))

    def test_20_merge_record_complex(self):
        """
        In this example we test if we merge different attributes into one node


        **Graph-Strucutre**

        .. figure:: https://cdn.rawgit.com/dlr-sc/prov-db-connector/master/docs/_images/test_cases/test_20_merge_record_complex.svg
           :align: center
           :scale: 50 %

        **Input-Data**

        This is the attributes used to create the entry

        .. code-block:: json

            {
               "ex:individual attribute":"Some value",
               "ex:dict value":{
                  "dict":"value"
               },
               "ex:double value":99.33,
               "ex:list value":[
                  "list",
                  "of",
                  "strings"
               ],
               "ex:int value":99,
               "ex:date value":"datetime.datetime(2005, 6, 1, 13, 33)"
            }

        This are the attributes to alter the existing node

        .. code-block:: json

            {
                "ex:a other attribute":true
            }

        **Output-Data**

        The output is one entry with the additional attribute

        .. code-block:: json

            [
               {
                  "ex:individual attribute":"Some value",
                  "ex:dict value":"{\"dict\": \"value\"}",
                  "ex:double value":99.33,
                  "ex:list value":[
                     "list",
                     "of",
                     "strings"
                  ],
                  "ex:int value":99,
                  "ex:date value":"2005-06-01 13:33:00",
                  "ex:a other attribute":true
               },
               {
                  "type_map":"{\"date value\": \"xds:datetime\", \"int value\": \"int\"}",
                  "identifier":"ex:Yoda",
                  "prov_type":"prov:Activity",
                  "namespaces":"{\"ex\": \"http://example.com\", \"custom\": \"http://custom.com\"}"
               }
            ]


        """
        self.clear_database()
        example = base_connector_merge_example()
        # Skip test if this merge mode is not supported

        # Save record with different attributes
        rec_id1 = self.instance.save_element(example.from_node["attributes"], example.from_node["metadata"])

        attr_modified = dict()
        attr_modified.update({"ex:a other attribute": True})
        metadata_modified = example.from_node["metadata"].copy()

        rec_id2 = self.instance.save_element(attr_modified, metadata_modified)

        self.assertEqual(rec_id1, rec_id2)

        # Test merge result of save record

        db_record = self.instance.get_record(rec_id2)  # The id's are equal so it makes no difference

        attr = encode_dict_values_to_primitive(example.from_node["attributes"])
        attr.update(attr_modified) # add additional attr to check dict
        meta = encode_dict_values_to_primitive(example.from_node["metadata"])

        db_meta = encode_adapter_result_to_excpect(db_record.metadata).copy()

        meta.update({METADATA_KEY_NAMESPACES: json.loads(meta[METADATA_KEY_NAMESPACES])})
        meta.update({METADATA_KEY_TYPE_MAP: json.loads(meta[METADATA_KEY_TYPE_MAP])})

        db_meta.update({METADATA_KEY_NAMESPACES: json.loads(db_meta[METADATA_KEY_NAMESPACES])})
        db_meta.update({METADATA_KEY_TYPE_MAP: json.loads(db_meta[METADATA_KEY_TYPE_MAP])})


        self.assertEqual(attr, db_record.attributes)
        self.assertEqual(meta, db_meta)

    def test_21_merge_record_complex_fail(self):
        """
         In this example we test if we merge different attributes into one node

        **Graph-Strucutre**

        .. figure:: https://cdn.rawgit.com/dlr-sc/prov-db-connector/master/docs/_images/test_cases/test_21_merge_record_complex_fail.svg
           :align: center
           :scale: 50 %

         **Input-Data**

         This is the attributes used to create the entry

         .. code-block:: json

             {
               "ex:list value":[
                  "list",
                  "of",
                  "strings"
               ],
               "ex:double value":99.33,
               "ex:date value":"datetime.datetime(2005, 6, 1, 13, 33)",
               "ex:dict value":{
                  "dict":"value"
               },
               "ex:int value":99,
               "ex:individual attribute":"Some value"
            }

         Try to *override* the existing attribute

         .. code-block:: json

            {
                "ex:int value": 1
            }

         **Output-Data**

         Should throw an MergeException

         """
        self.clear_database()
        example = base_connector_merge_example()
        # Skip test if this merge mode is not supported

        # Save record with different attributes
        rec_id1 = self.instance.save_element(example.from_node["attributes"], example.from_node["metadata"])

        attr_modified = dict()
        attr_modified.update({"ex:int value": 1}) # set int value to 1 instead of 99, should throw exception
        metadata_modified = example.from_node["metadata"].copy()

        #should raise exception because otherwise the attribute would be overridden
        with self.assertRaises(MergeException):
            self.instance.save_element(attr_modified, metadata_modified)

    def test_22_merge_record_metadata(self):
        """
        This test try to merge the metadata.
        This is important if you add some new attributes that uses other namespaces, so you need to merge the namespaces
        Same behavior for the type_map

        **Graph-Strucutre**

        .. figure:: https://cdn.rawgit.com/dlr-sc/prov-db-connector/master/docs/_images/test_cases/test_22_merge_record_metadata.svg
           :align: center
           :scale: 50 %

        **Input-Data**

        The metadata for the initial record:

        .. code-block:: json

             {
               "type_map":{
                  "date value":"xds:datetime",
                  "int value":"int"
               },
               "prov_type":"<QualifiedName: prov:Activity>",
               "namespaces":{
                  "custom":"http://custom.com",
                  "ex":"http://example.com"
               },
               "identifier":"<QualifiedName: ex:Yoda>"
            }


        Try to add a record with some modified namespaces

        .. code-block:: json

            {
               "namespaces":{
                  "custom":"http://custom.com",
                  "ex":"http://example.com"
               },
               "prov_type":"<QualifiedName: prov:Activity>",
               "identifier":"<QualifiedName: ex:Yoda>",
               "type_map":{
                  "custom_attr_1":"xds:some_value"
               }
            }


        **Output-Data**

        The output is the merged result of the type map

        .. code-block:: json

            [
               {
                  "ex:individual attribute":"Some value",
                  "ex:list value":[
                     "list",
                     "of",
                     "strings"
                  ],
                  "ex:int value":99,
                  "ex:date value":"2005-06-01 13:33:00",
                  "ex:dict value":"{\"dict\": \"value\"}",
                  "ex:double value":99.33
               },
               {
                  "prov_type":"prov:Activity",
                  "type_map":"{\"custom_attr_1\": \"xds:some_value\", \"date value\": \"xds:datetime\", \"int value\": \"int\"}",
                  "identifier":"ex:Yoda",
                  "namespaces":"{\"custom\": \"http://custom.com\", \"ex\": \"http://example.com\"}"
               }
            ]

        """
        self.clear_database()
        example = base_connector_merge_example()
        # Skip test if this merge mode is not supported

        # Save record with different attributes
        rec_id1 = self.instance.save_element(example.from_node["attributes"], example.from_node["metadata"])


        metadata_modified = example.from_node["metadata"].copy()
        metadata_modified.update({METADATA_KEY_TYPE_MAP: {"custom_attr_1": "xds:some_value"}})

        rec_id2 = self.instance.save_element(example.from_node["attributes"], metadata_modified)


        self.assertEqual(rec_id1, rec_id2)

        db_record = self.instance.get_record(rec_id1)
        # try to merge type_map in the database

        attr = encode_dict_values_to_primitive(example.relation["attributes"])
        meta = encode_dict_values_to_primitive(example.relation["metadata"])
        meta_custom = encode_dict_values_to_primitive(metadata_modified)

        # check namespaces
        db_record_namespaces = db_record.metadata[METADATA_KEY_NAMESPACES]




        if type(db_record_namespaces) is list:
            self.assertIsInstance(db_record_namespaces, list)
            self.assertTrue(len(db_record_namespaces), 2)
            self.assertEqual(meta[METADATA_KEY_NAMESPACES], db_record_namespaces[0])
            self.assertEqual(meta_custom[METADATA_KEY_NAMESPACES], db_record_namespaces[1])
        else:
            self.assertIsInstance(db_record_namespaces, str)
            decoded_map = json.loads(db_record_namespaces)
            expected_map = json.loads(meta[METADATA_KEY_NAMESPACES])
            expected_map.update(json.loads(meta_custom[METADATA_KEY_NAMESPACES]))
            self.assertEqual(decoded_map, expected_map)


        # check type_map
        db_record_type_map = db_record.metadata[METADATA_KEY_TYPE_MAP]

        if type(db_record_type_map) is list:
            self.assertIsInstance(db_record_type_map, list)
            self.assertTrue(len(db_record_type_map), 2)
            self.assertEqual(meta[METADATA_KEY_TYPE_MAP], db_record_type_map[0])
            self.assertEqual(meta_custom[METADATA_KEY_TYPE_MAP], db_record_type_map[1])
        else:
            self.assertIsInstance(db_record_type_map, str)
            decoded_map = json.loads(db_record_type_map)
            expected_map = json.loads(meta[METADATA_KEY_TYPE_MAP])
            expected_map.update(json.loads(meta_custom[METADATA_KEY_TYPE_MAP]))
            self.assertEqual(decoded_map, expected_map)



    def test_23_merge_relation(self):
        """
        Merge a relation is pretty similar to merge records.
        The big difference is the different rules for uniques

        **Graph-Strucutre**

        .. figure:: https://cdn.rawgit.com/dlr-sc/prov-db-connector/master/docs/_images/test_cases/test_23_merge_relation.svg
           :align: center
           :scale: 50 %

        A relation is unique if:

        - The relation type is the same
        - all other formal attributes (see SimpleDbAdapter) are the same

        otherwise it is not the same relation.


        """
        self.clear_database()
        example = base_connector_merge_example()
        prim = primer_example()
        self.assertEqual(len(prim.get_records()), len(prim.unified().get_records()))
        #save relation test
        self.instance.save_element(example.from_node["attributes"], example.from_node["metadata"])
        self.instance.save_element(example.to_node["attributes"], example.to_node["metadata"])

        from_label = example.from_node["metadata"][METADATA_KEY_IDENTIFIER]
        to_label = example.to_node["metadata"][METADATA_KEY_IDENTIFIER]

        rel_id1 = self.instance.save_relation(from_label,to_label,example.relation["attributes"], example.relation["metadata"])
        rel_id2 = self.instance.save_relation(from_label,to_label,example.relation["attributes"], example.relation["metadata"])

        self.assertEqual(rel_id1,rel_id2)

        # Test merge result of save relation

        db_record = self.instance.get_relation(rel_id2)  # The id's are equal so it makes no difference

        attr = encode_dict_values_to_primitive(example.relation["attributes"])
        meta = encode_dict_values_to_primitive(example.relation["metadata"])

        db_meta = encode_adapter_result_to_excpect(db_record.metadata).copy()

        meta.update({METADATA_KEY_NAMESPACES: json.loads(meta[METADATA_KEY_NAMESPACES])})
        meta.update({METADATA_KEY_TYPE_MAP: json.loads(meta[METADATA_KEY_TYPE_MAP])})

        db_meta.update({METADATA_KEY_NAMESPACES: json.loads(db_meta[METADATA_KEY_NAMESPACES])})
        db_meta.update({METADATA_KEY_TYPE_MAP: json.loads(db_meta[METADATA_KEY_TYPE_MAP])})


        self.assertEqual(attr, db_record.attributes)
        self.assertEqual(meta, db_meta)


    def test_24_merge_relation_complex(self):
        """
        Same behavior as the merge_node test

        """
        self.clear_database()
        example = base_connector_merge_example()
        prim = primer_example()
        self.assertEqual(len(prim.get_records()), len(prim.unified().get_records()))
        #save relation test
        self.instance.save_element(example.from_node["attributes"], example.from_node["metadata"])
        self.instance.save_element(example.to_node["attributes"], example.to_node["metadata"])

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

        db_meta = encode_adapter_result_to_excpect(db_record.metadata).copy()

        meta.update({METADATA_KEY_NAMESPACES: json.loads(meta[METADATA_KEY_NAMESPACES])})
        meta.update({METADATA_KEY_TYPE_MAP: json.loads(meta[METADATA_KEY_TYPE_MAP])})

        db_meta.update({METADATA_KEY_NAMESPACES: json.loads(db_meta[METADATA_KEY_NAMESPACES])})
        db_meta.update({METADATA_KEY_TYPE_MAP: json.loads(db_meta[METADATA_KEY_TYPE_MAP])})


        self.assertEqual(attr, db_record.attributes)
        self.assertEqual(meta, db_meta)


    def test_25_merge_relation_complex_fail(self):
        """
        Same behavior as the merge_node_fail test

        """
        self.clear_database()
        example = base_connector_merge_example()
        prim = primer_example()
        self.assertEqual(len(prim.get_records()), len(prim.unified().get_records()))
        # save relation test
        self.instance.save_element(example.from_node["attributes"], example.from_node["metadata"])
        self.instance.save_element(example.to_node["attributes"], example.to_node["metadata"])

        from_label = example.from_node["metadata"][METADATA_KEY_IDENTIFIER]
        to_label = example.to_node["metadata"][METADATA_KEY_IDENTIFIER]

        #try ot override a propertie in the database
        custom_attributes = dict()
        custom_attributes.update({"ex:int value": 1})

        rel_id1 = self.instance.save_relation(from_label, to_label, example.relation["attributes"],
                                              example.relation["metadata"])

        with self.assertRaises(MergeException):
            rel_id2 = self.instance.save_relation(from_label, to_label, custom_attributes, example.relation["metadata"])


    def test_26_merge_relation_metadata(self):
        """
        Same as the merge_record_metadata

        """
        self.clear_database()
        example = base_connector_merge_example()
        prim = primer_example()
        self.assertEqual(len(prim.get_records()), len(prim.unified().get_records()))
        # save relation test
        self.instance.save_element(example.from_node["attributes"], example.from_node["metadata"])
        self.instance.save_element(example.to_node["attributes"], example.to_node["metadata"])

        from_label = example.from_node["metadata"][METADATA_KEY_IDENTIFIER]
        to_label = example.to_node["metadata"][METADATA_KEY_IDENTIFIER]

        # try to merge type_map in the database
        custom_metadata = example.relation["metadata"].copy()
        custom_metadata.update({METADATA_KEY_TYPE_MAP: {"custom_attr_1": "xds:some_value"}})

        self.instance.save_relation(from_label, to_label, example.relation["attributes"],
                                              example.relation["metadata"])

        rel_id2 = self.instance.save_relation(from_label, to_label, example.relation["attributes"], custom_metadata)


        db_record = self.instance.get_relation(rel_id2)  # The id's are equal so it makes no difference


        attr = encode_dict_values_to_primitive(example.relation["attributes"])
        meta = encode_dict_values_to_primitive(example.relation["metadata"])
        meta_custom = encode_dict_values_to_primitive(custom_metadata)


        self.assertEqual(attr, db_record.attributes)

        # check namespaces
        db_record_namespaces = db_record.metadata[METADATA_KEY_NAMESPACES]

        if type(db_record_namespaces) is list:
            self.assertIsInstance(db_record_namespaces, list)
            self.assertTrue(len(db_record_namespaces), 2)
            self.assertEqual(meta[METADATA_KEY_NAMESPACES], db_record_namespaces[0])
            self.assertEqual(meta_custom[METADATA_KEY_NAMESPACES], db_record_namespaces[1])
        else:
            self.assertIsInstance(db_record_namespaces, str)
            decoded_map = json.loads(db_record_namespaces)
            expected_map = json.loads(meta[METADATA_KEY_NAMESPACES])
            expected_map.update(json.loads(meta_custom[METADATA_KEY_NAMESPACES]))
            self.assertEqual(decoded_map, expected_map)

        # check type_map
        db_record_type_map = db_record.metadata[METADATA_KEY_TYPE_MAP]

        if type(db_record_type_map) is list:
            self.assertIsInstance(db_record_type_map, list)
            self.assertTrue(len(db_record_type_map), 2)
            self.assertEqual(meta[METADATA_KEY_TYPE_MAP], db_record_type_map[0])
            self.assertEqual(meta_custom[METADATA_KEY_TYPE_MAP], db_record_type_map[1])
        else:
            self.assertIsInstance(db_record_type_map, str)
            decoded_map = json.loads(db_record_type_map)
            expected_map = json.loads(meta[METADATA_KEY_TYPE_MAP])
            expected_map.update(json.loads(meta_custom[METADATA_KEY_TYPE_MAP]))
            self.assertEqual(decoded_map, expected_map)



    #Rest of the tests
    def test_27_save_bundle(self):
        """
        .. warning:
            Don't start with this function

        :return:
        """
        self.clear_database()
        args = base_connector_bundle_parameter_example()
        id = self.instance.save_element(args["attributes"], args["metadata"])
        self.assertIsNotNone(id)
        self.assertIs(type(id), str, "id should be a string ")

class BaseConnectorTests(unittest.TestCase):
    """
    This class is only to test that the BaseConnector is alright

    """
    def test_instance_abstract_class(self):
        """
        Test that the BaseAdapter is abstract
        """
        base = BaseAdapter()
        with self.assertRaises(NotImplementedError):
            base.connect(None)