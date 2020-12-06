import os

from neo4j.exceptions import ConfigurationError
from neo4j.graph import Relationship

import provdbconnector.db_adapters.neo4j.cypher_commands as cypher_commands
from provdbconnector.db_adapters.baseadapter import BaseAdapter
from provdbconnector.db_adapters.baseadapter import METADATA_KEY_PROV_TYPE, METADATA_KEY_TYPE_MAP, \
    METADATA_KEY_IDENTIFIER, METADATA_KEY_NAMESPACES

from provdbconnector.exceptions.database import InvalidOptionsException, AuthException, \
    DatabaseException, CreateRecordException, NotFoundException, CreateRelationException, MergeException

from neo4j import GraphDatabase, basic_auth
from prov.constants import PROV_N_MAP
from collections import namedtuple
from provdbconnector.utils.serializer import encode_string_value_to_primitive, encode_dict_values_to_primitive, \
    split_into_formal_and_other_attributes

import logging

logging.getLogger("neo4j.bolt").setLevel(logging.WARN)
log = logging.getLogger(__name__)

NEO4J_USER = os.environ.get('NEO4J_USERNAME', 'neo4j')
NEO4J_PASS = os.environ.get('NEO4J_PASSWORD', 'neo4jneo4j')
NEO4J_HOST = os.environ.get('NEO4J_HOST', 'localhost')
NEO4J_BOLT_PORT = os.environ.get('NEO4J_BOLT_PORT', '7687')
NEO4J_HTTP_PORT = os.environ.get('NEO4J_HTTP_PORT', '7474')

NEO4J_META_PREFIX = "meta:"



class Neo4jAdapter(BaseAdapter):
    """
    This is the neo4j adapter to store prov. data in a neo4j database

    """
    def __init__(self, *args):
        """
        Setup the class

        :param args: None
        """
        super(Neo4jAdapter, self).__init__()
        self.driver = None
        pass

    def _create_session(self):
        """
        Get a session from the driver

        :return: Session
        :rtype Session
        """
        try:
            session = self.driver.session()
        except OSError as e:
            raise AuthException(e)

        return session

    def connect(self, authentication_options):
        """
        The connect method to create a new instance of the db_driver

        :param authentication_options: Username, password, host and encrypted option
        :return: None
        :rtype: None
        :raises: InvalidOptionsException
        """
        if authentication_options is None:
            raise InvalidOptionsException()

        user_name = authentication_options.get("user_name")
        user_pass = authentication_options.get("user_password")
        encrypted = authentication_options.get("encrypted")
        host = authentication_options.get("host")

        if encrypted is None:
            encrypted = False
        if user_name is None or user_pass is None or host is None:
            raise InvalidOptionsException()

        try:
            self.driver = GraphDatabase.driver("bolt://{}".format(host), encrypted=encrypted, auth=basic_auth(user_name, user_pass))

        except ConfigurationError as e:
            raise InvalidOptionsException(e)

        self._create_session()

    @staticmethod
    def _prefix_metadata(metadata):
        """
        Prefix all keys of a dict, only for the neo4j adapter

        :param metadata:
        :return: A dict with prefixed keys
        """
        prefixed_metadata = dict()
        for key, value in metadata.items():
            prefixed_metadata["{meta_prefix}{key}".format(key=key, meta_prefix=NEO4J_META_PREFIX)] = value

        return prefixed_metadata

    @staticmethod
    def _parse_to_primitive_attributes(attributes, prefixed_metadata):
        """
        Convert the dict values and keys into a neo4j friendly type (dict=>json, list,int,float, QualifiedName=>str, datetime=>str)

        :param attributes:
        :param prefixed_metadata:
        :return:
        """
        all_attributes = attributes.copy()
        all_attributes.update(prefixed_metadata)

        db_attributes = dict()
        # transform values
        for key, value in all_attributes.items():
            key_primitive = encode_string_value_to_primitive(key)
            value_primitive = encode_string_value_to_primitive(value)
            db_attributes.update({key_primitive: value_primitive})

        return db_attributes

    @staticmethod
    def _get_attributes_identifiers_cypher_string(key_list):
        """
        This function return a cypher string with all keys as cypher parameters

        :param key_list:
        :return:
        """
        db_attributes_identifiers = map(lambda key: "`{}`: {{`{}`}}".format(key, key), key_list)
        return ",".join(db_attributes_identifiers)

    @staticmethod
    def _get_attributes_set_cypher_string(key_list, cypher_template=cypher_commands.NEO4J_CREATE_NODE_SET_PART):
        """
        Returns a set cypher command for all keys of the keylist

        :param key_list:
        :param cypher_template:
        :return:
        """
        statements = list()
        for key in key_list:
            statements.append(cypher_template.format(attr_name=key))
        return " ".join(statements)

    def save_element(self, attributes, metadata):
        """
        Saves a single record

        :param attributes: The attributes dict
        :type attributes: dict
        :param metadata: The metadata dict
        :type metadata: dict
        :return: The id of the record
        :rtype: str
        """

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
        attr_for_simple_set = other_db_attribute_keys.copy()
        attr_for_simple_set.remove("meta:" + METADATA_KEY_NAMESPACES)
        attr_for_simple_set.remove("meta:" + METADATA_KEY_TYPE_MAP)
        cypher_set_statement = self._get_attributes_set_cypher_string(attr_for_simple_set)

        attr_for_list_merge = list()
        attr_for_list_merge.append("meta:" + METADATA_KEY_NAMESPACES)
        attr_for_list_merge.append("meta:" + METADATA_KEY_TYPE_MAP)
        cypher_set_statement += self._get_attributes_set_cypher_string(attr_for_list_merge,
                                                                       cypher_commands.NEO4J_CREATE_NODE_SET_PART_MERGE_ATTR)

        # get CASE WHEN ... statement to check if a attribute is different
        cypher_merge_check_statement = self._get_attributes_set_cypher_string(attr_for_simple_set,
                                                                              cypher_commands.NEO4J_CREATE_NODE_MERGE_CHECK_PART)

        # get cypher string for the merge relevant attributes
        cypher_merge_relevant_str = self._get_attributes_identifiers_cypher_string(merge_relevant_keys)

        # get prov type
        provtype = metadata[METADATA_KEY_PROV_TYPE]

        # get db_attributes as dict
        db_attributes = self._parse_to_primitive_attributes(attributes, prefixed_metadata)

        session = self._create_session()

        command = cypher_commands.NEO4J_CREATE_NODE_RETURN_ID.format(label=provtype.localpart,
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
                raise MergeException(
                    "The attributes {other} could not merged into the existing node, All attributes: {all} ".format(
                        other=other_db_attribute_keys, all=db_attributes))

        return str(record_id)

    def save_relation(self, from_node, to_node, attributes, metadata):
        """
        Save a single relation

        :param from_node: The from node as QualifiedName
        :type from_node: QualifiedName
        :param to_node: The to node as QualifiedName
        :type to_node: QualifiedName
        :param attributes: The attributes dict
        :type attributes: dict
        :param metadata: The metadata dict
        :type metadata: dict
        :return: Id of the relation
        :rtype: str
        """

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

        # Remove namespace and type_map from the direct set statement, because this attributes need to be merged
        attr_for_simple_set = other_db_attribute_keys.copy()
        attr_for_simple_set.remove("meta:" + METADATA_KEY_NAMESPACES)
        attr_for_simple_set.remove("meta:" + METADATA_KEY_TYPE_MAP)
        cypher_set_statement = self._get_attributes_set_cypher_string(attr_for_simple_set)

        # Add separate cypher command to merge the namespaces and tpye map into a list
        attr_for_list_merge = list()
        attr_for_list_merge.append("meta:" + METADATA_KEY_NAMESPACES)
        attr_for_list_merge.append("meta:" + METADATA_KEY_TYPE_MAP)
        cypher_set_statement += self._get_attributes_set_cypher_string(attr_for_list_merge,
                                                                       cypher_commands.NEO4J_CREATE_NODE_SET_PART_MERGE_ATTR)

        # get CASE WHEN ... statement to check if a attribute is different
        cypher_merge_check_statement = self._get_attributes_set_cypher_string(attr_for_simple_set,
                                                                              cypher_commands.NEO4J_CREATE_NODE_MERGE_CHECK_PART)

        # get cypher string for the merge relevant attributes
        cypher_merge_relevant_str = self._get_attributes_identifiers_cypher_string(merge_relevant_keys)

        # get db_attributes as dict
        db_attributes = self._parse_to_primitive_attributes(attributes, prefixed_metadata)

        with self._create_session() as session:

            relationtype = PROV_N_MAP[metadata[METADATA_KEY_PROV_TYPE]]

            command = cypher_commands.NEO4J_CREATE_RELATION_RETURN_ID.format(from_identifier=str(from_node),
                                                             to_identifier=str(to_node),
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

    @staticmethod
    def _split_attributes_metadata_from_node(db_node):
        """
        This functions splits a db node back into attributes and metadata, based on the prefix


        :param db_node:
        :type db_node: dict
        :return: namedTuple(attributes,metadata)
        """
        record = namedtuple('Record', 'attributes, metadata')
        # split data
        metadata = {k.replace(NEO4J_META_PREFIX, ""): v for k, v in db_node._properties.items() if
                    k.startswith(NEO4J_META_PREFIX, 0, len(NEO4J_META_PREFIX))}
        attributes = {k: v for k, v in db_node._properties.items() if
                      not k.startswith(NEO4J_META_PREFIX, 0, len(NEO4J_META_PREFIX))}

        # convert a list of namespace into a string if it is only one item
        # @todo Kind of a hack to pass all test, it is also allowed to return a list of JSON encoded strings
        namespaces = metadata[METADATA_KEY_NAMESPACES]
        if isinstance(namespaces, list):
            # If len is 1 return only the raw JSON string
            if len(namespaces) == 1:
                metadata.update({METADATA_KEY_NAMESPACES: namespaces.pop()})

        # convert a list of namespace into a string if it is only one item
        # @todo Kind of a hack to pass all test, it is also allowed to return a list of JSON encoded strings
        # @todo Find out what I hacked here in 2015...
        type_map = metadata[METADATA_KEY_TYPE_MAP]
        if isinstance(type_map, list):
            # If len is 1 return only the raw JSON string
            if len(type_map) == 1:
                metadata.update({METADATA_KEY_TYPE_MAP: type_map.pop()})

        record = record(attributes, metadata)
        return record

    def _get_cypher_filter_params(self, properties_dict, metadata_dict):
        """
        This functions returns a tuple with the cypher_str for the cypher filter and the right parameter names


        :param properties_dict: Search dict
        :param metadata_dict: Seacrh dict
        :return: Tuple(Keys with metadata prefix (if necessary), cypher filter str )
        """
        metadata_dict_prefixed = {"meta:{}".format(k): v for k, v in metadata_dict.items()}

        # Merge the 2 dicts into one
        filter = properties_dict.copy()
        filter.update(metadata_dict_prefixed)

        encoded_params = encode_dict_values_to_primitive(filter)
        cypher_str = self._get_attributes_identifiers_cypher_string(filter.keys())
        return encoded_params, cypher_str

    def get_records_by_filter(self, attributes_dict=None, metadata_dict=None):
        """
        Return the records by a certain filter

        :param attributes_dict: Filter dict
        :type attributes_dict: dict
        :param metadata_dict: Filter dict for metadata
        :type metadata_dict: dict
        :return: list of all nodes and relations that fit the conditions
        :rtype: list(DbRecord and DbRelation)
        """

        if attributes_dict is None:
            attributes_dict = dict()
        if metadata_dict is None:
            metadata_dict = dict()

        (encoded_params, cypher_str) = self._get_cypher_filter_params(attributes_dict, metadata_dict)

        session = self._create_session()
        records = list()
        result_set = session.run(cypher_commands.NEO4J_GET_RECORDS_BY_PROPERTY_DICT.format(filter_dict=cypher_str), encoded_params)
        for result in result_set:
            record = result["re"]

            if record is None:
                raise DatabaseException("Record response should not be None")
            relation_record = self._split_attributes_metadata_from_node(record)
            records.append(relation_record)
        return records

    def get_records_tail(self, attributes_dict=None, metadata_dict=None, depth=None):
        """
        Return all connected nodes form the origin.


        :param attributes_dict: Filter dict
        :type attributes_dict: dict
        :param metadata_dict: Filter dict for metadata
        :type metadata_dict: dict
        :param depth: Max steps
        :return: list of all nodes and relations that fit the conditions
        :rtype: list(DbRecord and DbRelation)
        """

        if attributes_dict is None:
            attributes_dict = dict()
        if metadata_dict is None:
            metadata_dict = dict()

        (encoded_params, cypher_str) = self._get_cypher_filter_params(attributes_dict, metadata_dict)

        depth_str = ""
        if depth is not None:
            depth_str = "1..{max}".format(max=depth)

        session = self._create_session()
        result_set = session.run(cypher_commands.NEO4J_GET_RECORDS_TAIL_BY_FILTER.format(filter_dict=cypher_str, depth=depth_str),
                                 encoded_params)
        records = list()
        for result in result_set:
            record = result["re"]

            if record is None:
                raise DatabaseException("Record response should not be None")
            relation_record = self._split_attributes_metadata_from_node(record)
            records.append(relation_record)

        return records

    def get_bundle_records(self, bundle_identifier):
        """
        Return all records and relations for the bundle


        :param bundle_identifier:
        :return:
        """

        session = self._create_session()
        result_set = session.run(cypher_commands.NEO4J_GET_BUNDLE_RECORDS,
                                 {'meta:{}'.format(METADATA_KEY_IDENTIFIER): str(bundle_identifier)})
        records = list()
        for result in result_set:
            record = result["re"]

            if record is None:
                raise DatabaseException("Record response should not be None")
            relation_record = self._split_attributes_metadata_from_node(record)
            records.append(relation_record)

        return records

    def get_record(self, record_id):
        """
        Try to find the record in the database

        :param record_id:
        :return: DbRecord
        :rtype: DbRecord
        """

        session = self._create_session()
        result_set = session.run(cypher_commands.NEO4J_GET_RECORD_RETURN_NODE, {"record_id": int(record_id)})

        node = None
        for result in result_set:
            if node is not None:
                raise DatabaseException(
                    "get_record should return only one node for the id {}, command {}".format(record_id,
                                                                                              cypher_commands.NEO4J_GET_RECORD_RETURN_NODE))
            node = result["node"]

        if node is None:
            raise NotFoundException("We cant find the node with the id: {}, database command {}".format(record_id,
                                                                                                        cypher_commands.NEO4J_GET_RECORD_RETURN_NODE))

        return self._split_attributes_metadata_from_node(node)

    def get_relation(self, relation_id):
        """
        Get a relation

        :param relation_id:
        :return: The relation
        :rtype: DbRelation
        """

        session = self._create_session()
        result_set = session.run(cypher_commands.NEO4J_GET_RELATION_RETURN_NODE, {"relation_id": int(relation_id)})

        relation = None
        for result in result_set:
            if not isinstance(result["relation"], Relationship):
                raise DatabaseException(
                    " should return only relationship {}, command {}".format(relation_id, cypher_commands.NEO4J_GET_RECORD_RETURN_NODE))

            relation = result["relation"]

        if relation is None:
            raise NotFoundException("We cant find the relation with the id: {}, database command {}".format(relation_id,
                                                                                                            cypher_commands.NEO4J_GET_RECORD_RETURN_NODE))

        return self._split_attributes_metadata_from_node(relation)

    def delete_records_by_filter(self, attributes_dict=None, metadata_dict=None):
        """
        Delete records and relations by a filter


        :param attributes_dict:
        :param metadata_dict:
        :return:
        """

        if attributes_dict is None:
            attributes_dict = dict()
        if metadata_dict is None:
            metadata_dict = dict()

        (encoded_params, cypher_str) = self._get_cypher_filter_params(attributes_dict, metadata_dict)
        session = self._create_session()

        session.run(cypher_commands.NEO4J_DELETE_NODE_BY_PROPERTIES.format(filter_dict=cypher_str), encoded_params)

        return True

    def delete_record(self, record_id):
        """
        Delete a single record


        :param record_id:
        :return:
        """
        session = self._create_session()
        session.run(cypher_commands.NEO4J_DELETE__NODE_BY_ID, {"node_id": int(record_id)})
        return True

    def delete_relation(self, relation_id):
        """
        Delete a single relation


        :param relation_id:
        :return:
        """
        session = self._create_session()
        session.run(cypher_commands.NEO4J_DELETE_RELATION_BY_ID, {"relation_id": int(relation_id)})
        return True
