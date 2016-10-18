import os
from provdbconnector.db_adapters.baseadapter import BaseAdapter
from provdbconnector.db_adapters.baseadapter import METADATA_KEY_PROV_TYPE, METADATA_KEY_TYPE_MAP, METADATA_KEY_IDENTIFIER, METADATA_KEY_NAMESPACES

from provdbconnector.exceptions.database import InvalidOptionsException, AuthException, \
    DatabaseException, CreateRecordException, NotFoundException, CreateRelationException, MergeException

from neo4j.v1.exceptions import ProtocolError
from neo4j.v1 import GraphDatabase, basic_auth, Relationship
from prov.constants import PROV_N_MAP
from collections import namedtuple
from provdbconnector.utils.serializer import encode_string_value_to_primitive, encode_dict_values_to_primitive,split_into_formal_and_other_attributes

import logging
logging.getLogger("neo4j.bolt").setLevel(logging.WARN)
log = logging.getLogger(__name__)

NEO4J_USER = os.environ.get('NEO4J_USERNAME', 'neo4j')
NEO4J_PASS = os.environ.get('NEO4J_PASSWORD', 'neo4jneo4j')
NEO4J_HOST = os.environ.get('NEO4J_HOST', 'localhost')
NEO4J_BOLT_PORT = os.environ.get('NEO4J_BOLT_PORT', '7687')
NEO4J_HTTP_PORT = os.environ.get('NEO4J_HTTP_PORT', '7474')

NEO4J_META_PREFIX = "meta:"

NEO4J_TEST_CONNECTION = """MATCH (n) RETURN count(n) as count"""

# create
NEO4J_CREATE_DOCUMENT_NODE_RETURN_ID = """CREATE (node { }) RETURN ID(node) as ID"""
NEO4J_CREATE_NODE_SET_PART = "SET node.`{attr_name}` = {{`{attr_name}`}}"
NEO4J_CREATE_NODE_SET_PART_MERGE_ATTR = "SET node.`{attr_name}` = (CASE WHEN not exists(node.`{attr_name}`) THEN [{{`{attr_name}`}}] ELSE node.`{attr_name}` + {{`{attr_name}`}}  END)"
NEO4J_CREATE_NODE_MERGE_CHECK_PART = """WITH CASE WHEN check = 0 THEN (CASE  WHEN EXISTS(node.`{attr_name}`) AND node.`{attr_name}` <> {{`{attr_name}`}} THEN 1 ELSE 0 END) ELSE 1 END as check , node """
NEO4J_CREATE_NODE_RETURN_ID = """MERGE (node:{label} {{{formal_attributes}}})
                                WITH 0 as check, node
                                {merge_check_statement}
                                {set_statement}
                                RETURN ID(node) as ID, check """  # args: provType, values
NEO4J_CREATE_RELATION_RETURN_ID = """
                                MATCH
                                    (from{{`meta:identifier`:'{from_identifier}'}}),
                                    (to{{`meta:identifier`:'{to_identifier}'}})
                                MERGE
                                    (from)-[r:{relation_type} {{{formal_attributes}}}]->(to)
                                    WITH 0 as check, r as node
                                    {merge_check_statement}
                                    {set_statement}
                                RETURN
                                    ID(node) as ID, check
                                """  # args: provType, values
# get
NEO4j_GET_BUNDLE_RETURN_BUNDLE_NODE = """
                        MATCH (b {`meta:prov_type`:'prov:Bundle'}) WHERE ID(b)=toInt({bundle_id}) RETURN b
                    """
NEO4J_Get_BUNDLES_RETURN_BUNDLE_IDS = """
                        MATCH (d {`meta:parent_id`:{parent_id}, `meta:prov_type`: 'prov:Bundle'}) Return id(d) as ID
                    """
NEO4J_GET_RECORDS_BY_PROPERTY_DICT= """
                            MATCH (d {{{filter_dict}}} )-[r]-(x {{{filter_dict}}})
                            RETURN DISTINCT r as re
                            //Get all nodes that are alone without connections to other nodes
                            UNION
                            MATCH (a {{{filter_dict}}})
                            RETURN DISTINCT a as re
                        """
NEO4J_GET_RECORDS_TAIL_BY_FILTER = """
                            MATCH (x {{{filter_dict}}})-[r *{depth}]-(y)
                            RETURN  DISTINCT y as re
                            UNION
                            MATCH (x {{{filter_dict}}})-[r *{depth}]-(y)
                            WITH REDUCE(output = [], r IN r | output + r) AS flat
                            UNWIND flat as re
                            RETURN DISTINCT re
                        """
NEO4J_GET_RECORD_RETURN_NODE = """MATCH (node) WHERE ID(node)={record_id} RETURN node"""
NEO4J_GET_RELATION_RETURN_NODE = """MATCH ()-[relation]-() WHERE ID(relation)={relation_id}  RETURN relation"""

# delete
NEO4J_DELETE__NODE_BY_ID = """MATCH  (x) Where ID(x) = {node_id} DETACH DELETE x """
NEO4J_DELETE_NODE_BY_PROPERTIES = """MATCH (n {{{filter_dict}}}) DETACH DELETE n"""
NEO4J_DELETE_BUNDLE_NODE_BY_ID = """MATCH (b) WHERE id(b)=toInt({bundle_id}) DELETE b """
NEO4J_DELETE_RELATION_BY_ID = """MATCH ()-[r]-() WHERE id(r) = {relation_id} DELETE r"""


class Neo4jAdapter(BaseAdapter):
    def __init__(self, *args):
        super(Neo4jAdapter, self).__init__()
        self.driver = None
        pass

    def _create_session(self):
        try:
            session = self.driver.session()
        except OSError as e:
            raise AuthException(e)

        if not session.healthy:
            raise AuthException()
        return session

    def connect(self, authentication_options):
        if authentication_options is None:
            raise InvalidOptionsException()

        user_name = authentication_options.get("user_name")
        user_pass = authentication_options.get("user_password")
        host = authentication_options.get("host")

        if user_name is None or user_pass is None or host is None:
            raise InvalidOptionsException()

        try:
            self.driver = GraphDatabase.driver("bolt://{}".format(host), auth=basic_auth(user_name, user_pass))

        except ProtocolError as e:
            raise InvalidOptionsException(e)

        self._create_session()

    def _prefix_metadata(self, metadata):
        prefixed_metadata = dict()
        for key, value in metadata.items():
            prefixed_metadata["{meta_prefix}{key}".format(key=key, meta_prefix=NEO4J_META_PREFIX)] = value

        return prefixed_metadata

    def _parse_to_primitive_attributes(self, attributes, prefixed_metadata):
        all_attributes = attributes.copy()
        all_attributes.update(prefixed_metadata)

        db_attributes = dict()
        # transform values
        for key, value in all_attributes.items():
            key_primitive = encode_string_value_to_primitive(key)
            value_primitive = encode_string_value_to_primitive(value)
            db_attributes.update({key_primitive: value_primitive})

        return db_attributes

    def _get_attributes_identifiers_cypher_string(self, key_list):
        db_attributes_identifiers = map(lambda key: "`{}`: {{`{}`}}".format(key, key), key_list)
        return ",".join(db_attributes_identifiers)

    def _get_attributes_set_cypher_string(self,key_list, CYPHER_TEMPLATE = NEO4J_CREATE_NODE_SET_PART):
        statements = list()
        for key in key_list:
            statements.append(CYPHER_TEMPLATE.format(attr_name=key))

        return " ".join(statements)
    def save_record(self, attributes, metadata):

        metadata = metadata.copy()

        prefixed_metadata = self._prefix_metadata(metadata)

        #setup merge attributes
        (formal_attributes, other_attributes) = split_into_formal_and_other_attributes(attributes,metadata)

        merge_relevant_keys = list()
        merge_relevant_keys.append("meta:{}".format(METADATA_KEY_IDENTIFIER))
        merge_relevant_keys = merge_relevant_keys + list(formal_attributes.keys())

        other_db_attribute_keys = list()
        other_db_attribute_keys = other_db_attribute_keys + list(other_attributes.keys())
        other_db_attribute_keys = other_db_attribute_keys + list(prefixed_metadata.keys())

        #get set statement for non formal attributes
        attr_for_simple_set = other_db_attribute_keys.copy()
        attr_for_simple_set.remove("meta:"+METADATA_KEY_NAMESPACES)
        attr_for_simple_set.remove("meta:"+METADATA_KEY_TYPE_MAP)
        cypher_set_statement = self._get_attributes_set_cypher_string(attr_for_simple_set)

        attr_for_list_merge = list()
        attr_for_list_merge.append("meta:"+METADATA_KEY_NAMESPACES)
        attr_for_list_merge.append("meta:"+METADATA_KEY_TYPE_MAP)
        cypher_set_statement = cypher_set_statement + self._get_attributes_set_cypher_string(attr_for_list_merge, NEO4J_CREATE_NODE_SET_PART_MERGE_ATTR)

        #get CASE WHEN ... statement to check if a attribute is different
        cypher_merge_check_statement = self._get_attributes_set_cypher_string(attr_for_simple_set,NEO4J_CREATE_NODE_MERGE_CHECK_PART)

        #get cypher string for the merge relevant attributes
        cypher_merge_relevant_str = self._get_attributes_identifiers_cypher_string(merge_relevant_keys)

        #get prov type
        provtype = metadata[METADATA_KEY_PROV_TYPE]


        #get db_attributes as dict
        db_attributes = self._parse_to_primitive_attributes(attributes, prefixed_metadata)

        session = self._create_session()


        command = NEO4J_CREATE_NODE_RETURN_ID.format(label=provtype.localpart,
                                                     formal_attributes=cypher_merge_relevant_str,
                                                     set_statement=cypher_set_statement,
                                                     merge_check_statement=cypher_merge_check_statement)
        with session.begin_transaction() as tx:

            result = tx.run(command, dict(db_attributes))

            record_id = None
            merge_success = 0
            for record in result:
                record_id = record["ID"]
                merge_success = record["check"]


            if record_id is None:
                raise CreateRecordException("No ID property returned by database for the command {}".format(command))
            if merge_success == 0:
                tx.success = True
            else:
                tx.success = False
                raise MergeException("The attributes {other} could not merged into the existing node, All attributes: {all} ".format(other=other_db_attribute_keys,all=db_attributes))

        return str(record_id)

    def save_relation(self,  from_node,  to_node, attributes, metadata):

        metadata = metadata.copy()

        prefixed_metadata = self._prefix_metadata(metadata)

        # setup merge attributes
        (formal_attributes, other_attributes) = split_into_formal_and_other_attributes(attributes, metadata)

        merge_relevant_keys = list()
        merge_relevant_keys.append("meta:{}".format(METADATA_KEY_IDENTIFIER))
        merge_relevant_keys = merge_relevant_keys + list(formal_attributes.keys())

        other_db_attribute_keys = list()
        other_db_attribute_keys = other_db_attribute_keys + list(other_attributes.keys())
        other_db_attribute_keys = other_db_attribute_keys + list(prefixed_metadata.keys())

        # get set statement for non formal attributes

        #Remove namespace and type_map from the direct set statement, because this attributes need to be merged
        attr_for_simple_set = other_db_attribute_keys.copy()
        attr_for_simple_set.remove("meta:" + METADATA_KEY_NAMESPACES)
        attr_for_simple_set.remove("meta:" + METADATA_KEY_TYPE_MAP)
        cypher_set_statement = self._get_attributes_set_cypher_string(attr_for_simple_set)

        #Add separate cypher command to merge the namespaces and tpye map into a list
        attr_for_list_merge = list()
        attr_for_list_merge.append("meta:" + METADATA_KEY_NAMESPACES)
        attr_for_list_merge.append("meta:" + METADATA_KEY_TYPE_MAP)
        cypher_set_statement = cypher_set_statement + self._get_attributes_set_cypher_string(attr_for_list_merge,
                                                                                             NEO4J_CREATE_NODE_SET_PART_MERGE_ATTR)

        # get CASE WHEN ... statement to check if a attribute is different
        cypher_merge_check_statement = self._get_attributes_set_cypher_string(attr_for_simple_set,
                                                                              NEO4J_CREATE_NODE_MERGE_CHECK_PART)

        # get cypher string for the merge relevant attributes
        cypher_merge_relevant_str = self._get_attributes_identifiers_cypher_string(merge_relevant_keys)


        # get db_attributes as dict
        db_attributes = self._parse_to_primitive_attributes(attributes, prefixed_metadata)

        session = self._create_session()

        relationtype = PROV_N_MAP[metadata[METADATA_KEY_PROV_TYPE]]

        command = NEO4J_CREATE_RELATION_RETURN_ID.format( from_identifier=from_node,
                                                          to_identifier=to_node,
                                                          relation_type=relationtype,
                                                          formal_attributes=cypher_merge_relevant_str,
                                                          merge_check_statement=cypher_merge_check_statement,
                                                          set_statement=cypher_set_statement
                                                         )
        with session.begin_transaction() as tx:
            result = tx.run(command, dict(db_attributes))

            record_id = None
            merge_success = 0
            for record in result:
                record_id = record["ID"]
                merge_success = record["check"]

            if record_id is None:
                raise CreateRelationException("No ID property returned by database for the command {}".format(command))
            if merge_success == 0:
                tx.success = True
            else:
                tx.success = False
                raise MergeException("The attributes {other} could not merged into the existing node ".format(
                    other=other_db_attribute_keys))

        return str(record_id)

    def _split_attributes_metadata_from_node(self, db_node):
        record = namedtuple('Record', 'attributes, metadata')
        # split data
        metadata = {k.replace(NEO4J_META_PREFIX, ""): v for k, v in db_node.properties.items() if
                    k.startswith(NEO4J_META_PREFIX, 0, len(NEO4J_META_PREFIX))}
        attributes = {k: v for k, v in db_node.properties.items() if
                      not k.startswith(NEO4J_META_PREFIX, 0, len(NEO4J_META_PREFIX))}


        #convert a list of namespace into a string if it is only one item
        #@todo Kind of a hack to pass all test, it is also allowed to return a list of JSON encoded strings
        namespaces = metadata[METADATA_KEY_NAMESPACES]
        if isinstance(namespaces,list):
            #If len is 1 return only the raw JSON string
            if len(namespaces) is 1:
                metadata.update({METADATA_KEY_NAMESPACES: namespaces.pop()})

        #convert a list of namespace into a string if it is only one item
        #@todo Kind of a hack to pass all test, it is also allowed to return a list of JSON encoded strings
        type_map = metadata[METADATA_KEY_TYPE_MAP]
        if isinstance(type_map,list):
            #If len is 1 return only the raw JSON string
            if len(type_map) is 1:
                metadata.update({METADATA_KEY_TYPE_MAP: type_map.pop()})


        record = record(attributes, metadata)
        return record

    def decode_string_value_to_primitive(self, attributes, metadata):
        type_map = metadata[METADATA_KEY_TYPE_MAP]

    def get_bundle_ids(self, document_id):
        session = self._create_session()

        result_set = session.run(NEO4J_Get_BUNDLES_RETURN_BUNDLE_IDS, {"parent_id": document_id})

        ids = list()
        for result in result_set:
            ids.append(result["ID"])

        return ids

    def _get_cypher_filter_params(self,properties_dict,metadata_dict):
        metadata_dict_prefixed = {"meta:{}".format(k): v for k, v in metadata_dict.items()}

        #Merge the 2 dicts into one
        filter = properties_dict.copy()
        filter.update(metadata_dict_prefixed)

        encoded_params = encode_dict_values_to_primitive(filter)
        cypher_str = self._get_attributes_identifiers_cypher_string(filter.keys())
        return (encoded_params,cypher_str)

    def get_records_by_filter(self,properties_dict=dict(),metadata_dict=dict()):

        (encoded_params ,cypher_str ) = self._get_cypher_filter_params(properties_dict,metadata_dict)

        session = self._create_session()
        records = list()
        result_set = session.run(NEO4J_GET_RECORDS_BY_PROPERTY_DICT.format(filter_dict=cypher_str), encoded_params)
        for result in result_set:
            record = result["re"]

            if record is None:
                raise DatabaseException("Record response should not be None")
            relation_record = self._split_attributes_metadata_from_node(record)
            records.append(relation_record)
        return records

    def get_records_tail(self,properties_dict=dict(), metadata_dict=dict(), depth=None):

        (encoded_params, cypher_str) = self._get_cypher_filter_params(properties_dict, metadata_dict)

        depth_str =""
        if depth is not None:
            depth_str = "1..{max}".format(max=depth)

        session = self._create_session()
        result_set = session.run(NEO4J_GET_RECORDS_TAIL_BY_FILTER.format(filter_dict=cypher_str, depth=depth_str), encoded_params)
        records = list()
        for result in result_set:
            record = result["re"]

            if record is None:
                raise DatabaseException("Record response should not be None")
            relation_record = self._split_attributes_metadata_from_node(record)
            records.append(relation_record)

        return records

    def get_record(self, record_id):

        session = self._create_session()
        result_set = session.run(NEO4J_GET_RECORD_RETURN_NODE, {"record_id": int(record_id)})

        node = None
        for result in result_set:
            if node is not None:
                raise DatabaseException(
                    "get_record should return only one node for the id {}, command {}".format(record_id,
                                                                                              NEO4J_GET_RECORD_RETURN_NODE))
            node = result["node"]

        if node is None:
            raise NotFoundException("We cant find the node with the id: {}, database command {}".format(record_id,
                                                                                                        NEO4J_GET_RECORD_RETURN_NODE))

        return self._split_attributes_metadata_from_node(node)

    def get_relation(self, relation_id):

        session = self._create_session()
        result_set = session.run(NEO4J_GET_RELATION_RETURN_NODE, {"relation_id": int(relation_id)})

        relation = None
        for result in result_set:
            if not isinstance(result["relation"], Relationship):
                raise DatabaseException(
                    " should return only relationship {}, command {}".format(relation_id, NEO4J_GET_RECORD_RETURN_NODE))

            relation = result["relation"]

        if relation is None:
            raise NotFoundException("We cant find the relation with the id: {}, database command {}".format(relation_id,
                                                                                                            NEO4J_GET_RECORD_RETURN_NODE))

        return self._split_attributes_metadata_from_node(relation)

    def delete_records_by_filter(self, properties_dict=dict(), metadata_dict=dict()):

        (encoded_params, cypher_str) = self._get_cypher_filter_params(properties_dict, metadata_dict)
        session = self._create_session()

        result = session.run(NEO4J_DELETE_NODE_BY_PROPERTIES.format(filter_dict=cypher_str), encoded_params)

        return True


    def delete_record(self, record_id):
        session = self._create_session()
        result_set = session.run(NEO4J_DELETE__NODE_BY_ID, {"node_id": int(record_id)})
        return True

    def delete_relation(self, relation_id):
        session = self._create_session()
        result_set = session.run(NEO4J_DELETE_RELATION_BY_ID, {"relation_id": int(relation_id)})
        return True
