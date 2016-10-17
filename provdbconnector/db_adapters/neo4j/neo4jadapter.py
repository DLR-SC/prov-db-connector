import os
from provdbconnector.db_adapters.baseadapter import BaseAdapter
from provdbconnector.db_adapters.baseadapter import METADATA_KEY_PROV_TYPE, METADATA_KEY_TYPE_MAP

from provdbconnector.exceptions.database import InvalidOptionsException, AuthException, \
    DatabaseException, CreateRecordException, NotFoundException, CreateRelationException

from neo4j.v1.exceptions import ProtocolError
from neo4j.v1 import GraphDatabase, basic_auth, Relationship
from prov.constants import PROV_N_MAP
from collections import namedtuple
from provdbconnector.utils.serializer import encode_string_value_to_primitive, encode_dict_values_to_primitive

import logging
logging.getLogger("neo4j.bolt").setLevel(logging.WARN)
log = logging.getLogger(__name__)

NEO4J_USER = os.environ.get('NEO4J_USERNAME', 'neo4j')
NEO4J_PASS = os.environ.get('NEO4J_PASSWORD', 'neo4jneo4j')
NEO4J_HOST = os.environ.get('NEO4J_HOST', 'localhost')
NEO4J_BOLT_PORT = os.environ.get('NEO4J_BOLT_PORT', '7687')
NEO4J_HTTP_PORT = os.environ.get('NEO4J_HTTP_PORT', '7474')

NEO4J_META_PREFIX = "meta:"

NEO4J_META_BUNDLE_ID = "bundle_id"

NEO4J_META_PARENT_ID = "parent_id"

NEO4J_TEST_CONNECTION = """MATCH (n) RETURN count(n) as count"""

# create
NEO4J_CREATE_DOCUMENT_NODE_RETURN_ID = """CREATE (node { }) RETURN ID(node) as ID"""
NEO4J_CREATE_NODE_RETURN_ID = """MERGE (node:{label} {{{formal_attributes}}}) RETURN ID(node) as ID """  # args: provType, values
NEO4J_CREATE_RELATION_RETURN_ID = """
                                MATCH
                                    (from{{`meta:identifier`:'{from_identifier}'}}),
                                    (to{{`meta:identifier`:'{to_identifier}'}})
                                CREATE
                                    (from)-[r:{relation_type} {{{property_identifiers}}}]->(to)
                                RETURN
                                    ID(r) as ID
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

        session = self.driver.session()

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

    def _get_attributes_identifiers_cypher_string(self, db_attributes):
        db_attributes_identifiers = map(lambda key: "`{}`: {{`{}`}}".format(key, key), list(db_attributes.keys()))
        return ",".join(db_attributes_identifiers)

    def save_document(self):
        session = self._create_session()
        result = session.run(NEO4J_CREATE_DOCUMENT_NODE_RETURN_ID)
        record_id = None
        for record in result:
            record_id = record["ID"]

        result_delete = session.run(NEO4J_DELETE__NODE_BY_ID, {"node_id": record_id})

        if record_id is None:
            raise DatabaseException("Could not get a valid ID result back")

        return str(record_id + 1)

    def save_bundle(self, document_id, attributes, metadata):
        metadata = metadata.copy()
        metadata.update({NEO4J_META_PARENT_ID: document_id})
        return self.save_record(document_id, attributes, metadata)

    def save_record(self, attributes, metadata):

        metadata = metadata.copy()

        prefixed_metadata = self._prefix_metadata(metadata)

        db_attributes = self._parse_to_primitive_attributes(attributes, prefixed_metadata)

        provtype = metadata[METADATA_KEY_PROV_TYPE]

        identifier_str = self._get_attributes_identifiers_cypher_string(db_attributes)

        session = self._create_session()

        command = NEO4J_CREATE_NODE_RETURN_ID.format(label=provtype.localpart, formal_attributes=identifier_str)
        result = session.run(command, dict(db_attributes))

        record_id = None
        for record in result:
            record_id = record["ID"]

        if record_id is None:
            raise CreateRecordException("No ID property returned by database for the command {}".format(command))

        return str(record_id)

    def save_relation(self,  from_node,  to_node, attributes, metadata):

        metadata = metadata.copy()

        prefixed_metadata = self._prefix_metadata(metadata)

        db_attributes = self._parse_to_primitive_attributes(attributes, prefixed_metadata)

        relationtype = PROV_N_MAP[metadata[METADATA_KEY_PROV_TYPE]]

        identifier_str = self._get_attributes_identifiers_cypher_string(db_attributes)

        session = self._create_session()

        command = NEO4J_CREATE_RELATION_RETURN_ID.format(from_identifier=from_node,
                                                         to_identifier=to_node,
                                                         relation_type=relationtype,
                                                         property_identifiers=identifier_str)
        result = session.run(command, dict(db_attributes))

        record_id = None
        for record in result:
            record_id = record["ID"]

        if record_id is None:
            raise CreateRelationException("No ID property returned by database for the command {}".format(command))

        return str(record_id)

    def _split_attributes_metadata_from_node(self, db_node):
        record = namedtuple('Record', 'attributes, metadata')
        # split data
        metadata = {k.replace(NEO4J_META_PREFIX, ""): v for k, v in db_node.properties.items() if
                    k.startswith(NEO4J_META_PREFIX, 0, len(NEO4J_META_PREFIX))}
        attributes = {k: v for k, v in db_node.properties.items() if
                      not k.startswith(NEO4J_META_PREFIX, 0, len(NEO4J_META_PREFIX))}

        # remove adapter specific code
        if metadata.get(NEO4J_META_BUNDLE_ID) is not None:
            del metadata[NEO4J_META_BUNDLE_ID]

        if metadata.get(NEO4J_META_PARENT_ID) is not None:
            del metadata[NEO4J_META_PARENT_ID]

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
        cypher_str = self._get_attributes_identifiers_cypher_string(filter)
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
