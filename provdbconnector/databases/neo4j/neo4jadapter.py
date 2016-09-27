from provdbconnector.databases.baseadapter import BaseAdapter, InvalidOptionsException, AuthException, DatabaseException,CreateRecordException,NotFoundException,CreateRelationException,METADATA_PARENT_ID,METADATA_KEY_LABEL,METADATA_KEY_PROV_TYPE,METADATA_KEY_TYPE_MAP, METADATA_KEY_BUNDLE_ID

from neo4j.v1.exceptions import ProtocolError
from neo4j.v1 import GraphDatabase, basic_auth,Relationship
from prov.constants import  PROV_N_MAP
from collections import namedtuple
from provdbconnector.utils.serializer import encode_string_value_to_primitive

NEO4J_META_PREFIX = "meta:"

NEO4J_TEST_CONNECTION = """MATCH (n) RETURN count(n) as count"""

# create
NEO4J_CREATE_DOCUMENT_NODE_RETURN_ID = """CREATE (node { }) RETURN ID(node) as ID"""
NEO4J_CREATE_NODE_RETURN_ID = """CREATE (node:%s { %s}) RETURN ID(node) as ID """  # args: provType, values
NEO4J_CREATE_RELATION_RETURN_ID = """
                                MATCH
                                    (from{{`meta:bundle_id`:'{bundle_id}',`meta:label`:'{from_label}'}}),
                                    (to{{`meta:bundle_id`:'{bundle_id}', `meta:label`:'{to_label}'}})
                                CREATE
                                    (from)-[r:{relation_type} {{{property_labels}}}]->(to)
                                RETURN
                                    ID(r) as ID
                                """  # args: provType, values
#get
NEO4j_GET_BUNDLE_RETURN_BUNDLE_NODE = """MATCH (b {`meta:prov_type`:'prov:Bundle', `meta:bundle_id`: {bundle_id}}) RETURN b """
NEO4J_Get_BUNDLES_RETURN_BUNDLE_IDS = """MATCH (d {`meta:parent_id`:{parent_id}, `meta:prov_type`: 'prov:Bundle'}) Return id(d) as ID"""
NEO4J_GET_BUNDLE_RETURN_NODES_RELATIONS = """

                            MATCH (d)-[r]-(x)
                            WHERE not((d)-[:includeIn]-(x)) and not(d.`meta:prov_type`='prov:Bundle' or x.`meta:prov_type`='prov:Bundle')and (d.`meta:bundle_id`) ={bundle_id}
                            RETURN d as from, r as rel, x as to
                            //Get all nodes that are alone without connections to other
                            UNION
                            MATCH (a) WHERE (a.`meta:bundle_id`)={bundle_id}  AND NOT (a)<-[]->() and not(a.`meta:prov_type`='prov:Bundle')
                            RETURN a as from, NULL as rel, NULL as to
                            UNION
                            //Get all nodes that have only the includeIn connection to the bundle
                            MATCH (a)-[r:includeIn]->()
                            WITH a,count(r) as relation_count
                            WHERE (a.`meta:bundle_id`)={bundle_id} and NOT(a.`meta:prov_type`='prov:Bundle') AND relation_count=1
                            RETURN a as from,NULL as rel, NULL as to
                        """
NEO4J_GET_RECORD_RETURN_NODE  = """MATCH (node) WHERE ID(node)={record_id} RETURN node"""
NEO4J_GET_RELATION_RETURN_NODE  = """MATCH ()-[relation]-() WHERE ID(relation)={relation_id}  RETURN relation"""

# delete
NEO4J_DELETE__NODE_BY_ID = """MATCH  (x) Where ID(x) = {node_id} DETACH DELETE x """
NEO4J_DELETE_BUNDLE_BY_ID = """MATCH (d {`meta:bundle_id`:{bundle_id}}) DETACH DELETE  d"""


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

        user_name = authentication_options.get("user_name", None)
        user_pass = authentication_options.get("user_password", None)
        host = authentication_options.get("host", None)

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
        db_attributes = attributes.copy()
        db_attributes.update(prefixed_metadata)

        # transform values
        for key, value in db_attributes.items():
            db_attributes[key] = encode_string_value_to_primitive(value)
        return db_attributes

    def _get_attributes_labels_cypher_string(self, db_attributes):
        db_attributes_labels = map(lambda key: "`%s`: {`%s`}" % (key, key), list(db_attributes.keys()))

        return ",".join(db_attributes_labels)

    def create_document(self):
        session = self._create_session()
        result = session.run(NEO4J_CREATE_DOCUMENT_NODE_RETURN_ID)
        id = None
        for record in result:
            id = record["ID"]

        result_delete = session.run(NEO4J_DELETE__NODE_BY_ID, {"node_id": id})

        if id is None:
            raise DatabaseException("Could not get a valid ID result back")

        return str(id + 1)

    def create_bundle(self, document_id, attributes, metadata):
        metadata.update({METADATA_PARENT_ID: document_id})
        return self.create_record(document_id, attributes, metadata)

    def create_record(self, bundle_id, attributes, metadata):

        metadata = metadata.copy()
        metadata.update({METADATA_KEY_BUNDLE_ID: bundle_id})

        prefixed_metadata = self._prefix_metadata(metadata)

        db_attributes = self._parse_to_primitive_attributes(attributes, prefixed_metadata)

        provType = metadata[METADATA_KEY_PROV_TYPE]

        label_str = self._get_attributes_labels_cypher_string(db_attributes)

        session = self._create_session()

        command = NEO4J_CREATE_NODE_RETURN_ID % (provType.localpart, label_str)
        result = session.run(command, dict(db_attributes))

        id = None
        for record in result:
            id = record["ID"]

        if id is None:
            raise CreateRecordException("No ID property returned by database for the command {}".format(command))

        return str(id)

    def create_relation(self, bundle_id, from_node, to_node, attributes, metadata):

        metadata = metadata.copy()
        metadata.update({METADATA_KEY_BUNDLE_ID: bundle_id})

        prefixed_metadata = self._prefix_metadata(metadata)

        db_attributes = self._parse_to_primitive_attributes(attributes, prefixed_metadata)

        relationType = PROV_N_MAP[metadata[METADATA_KEY_PROV_TYPE]]

        label_str = self._get_attributes_labels_cypher_string(db_attributes)

        session = self._create_session()

        command = NEO4J_CREATE_RELATION_RETURN_ID.format(bundle_id=bundle_id,
                                                         from_label=from_node,
                                                         to_label=to_node,
                                                         relation_type=relationType,
                                                         property_labels=label_str)
        result = session.run(command, dict(db_attributes))

        id = None
        for record in result:
            id = record["ID"]

        if id is None:
            raise CreateRelationException("No ID property returned by database for the command {}".format(command))

        return str(id)

    def _split_attributes_metadata_from_node(self,db_node):
        Record = namedtuple('Record', 'attributes, metadata')
        metadata = {k.replace(NEO4J_META_PREFIX,""): v for k, v in db_node.properties.items() if k.startswith(NEO4J_META_PREFIX, 0,len(NEO4J_META_PREFIX)) }
        attributes = {k: v for k, v in db_node.properties.items() if not k.startswith(NEO4J_META_PREFIX,0,len(NEO4J_META_PREFIX)) }
        record = Record(attributes,metadata)
        return record

    def decode_string_value_to_primitive(self,attributes,metadata):
        type_map = metadata[METADATA_KEY_TYPE_MAP]


    def get_document(self, document_id):

        Document = namedtuple('Document', 'document, bundles')

        document = self.get_bundle(document_id)
        doc = Document(document,list())

        return doc

    def get_bundle_ids(self,document_id):
        session = self._create_session()

        result_set = session.run(NEO4J_Get_BUNDLES_RETURN_BUNDLE_IDS,{"parent_id": document_id})

        ids = list()
        for result in result_set:
            ids.append(result["ID"])

        return ids


    def get_bundle(self, bundle_id):

        session = self._create_session()
        records = list()
        result_set = session.run(NEO4J_GET_BUNDLE_RETURN_NODES_RELATIONS,{"bundle_id":bundle_id})
        for result in result_set:
            from_node   = result["from"]
            to_node     = result["to"]
            relation    = result["rel"]

            if from_node is not None:
                from_record_raw = self._split_attributes_metadata_from_node(from_node)
                records.append(from_record_raw)

            if to_node is not None:
                to_node_record = self._split_attributes_metadata_from_node(to_node)
                records.append(to_node_record)
            if relation is not None:
                relation_record = self._split_attributes_metadata_from_node(relation)
                records.append(relation_record)

        #Get bundle node and set identifier if there is a bundle node.
        bundle_node_result = session.run(NEO4j_GET_BUNDLE_RETURN_BUNDLE_NODE,{"bundle_id": bundle_id})
        identifier = None
        raw_record = None
        for bundle in bundle_node_result:
            raw_record = self._split_attributes_metadata_from_node(bundle["b"])
            identifier = raw_record.metadata[METADATA_KEY_LABEL]

        if raw_record is None and len(records) == 0:
            raise NotFoundException("bundle with the id {} was not found ".format(bundle_id))

        Bundle = namedtuple('Bundle', 'identifier, records, bundle_record')

        return Bundle(identifier, records,raw_record)

    def get_record(self,record_id):

        session = self._create_session()
        result_set = session.run(NEO4J_GET_RECORD_RETURN_NODE,{"record_id": int(record_id)})

        node = None
        for result in result_set:
            if node is not None:
                raise DatabaseException("get_record should return only one node for the id {}, command {}".format(record_id, NEO4J_GET_RECORD_RETURN_NODE))
            node = result["node"]

        if node is None:
            raise NotFoundException("We cant find the node with the id: {}, database command {}".format(record_id,NEO4J_GET_RECORD_RETURN_NODE))

        return self._split_attributes_metadata_from_node(node)

    def get_relation(self, relation_id):

        session = self._create_session()
        result_set = session.run(NEO4J_GET_RELATION_RETURN_NODE,{"relation_id": int(relation_id)})

        relation = None
        for result in result_set:
            if not isinstance(result["relation"],Relationship):
                raise DatabaseException(" should return only relationship {}, command {}".format(relation_id, NEO4J_GET_RECORD_RETURN_NODE))

            relation = result["relation"]

        if relation is None:
            raise NotFoundException("We cant find the relation with the id: {}, database command {}".format(relation_id,NEO4J_GET_RECORD_RETURN_NODE))

        return self._split_attributes_metadata_from_node(relation)

    def delete_document(self, document_id):
        bundleIds = self.get_bundle_ids(document_id)
        result_list = list()
        for id in bundleIds:
            result_list.append(self.delete_bundle(id))

        result_list.append(self.delete_bundle(document_id))

        return all(result_list)

    def delete_bundle(self, bundle_id):
        session = self._create_session()

        result_set = session.run(NEO4J_DELETE_BUNDLE_BY_ID,{"bundle_id": bundle_id})

        return True

    def delete_record(self,record_id):
        session = self._create_session()
        result_set = session.run(NEO4J_DELETE__NODE_BY_ID, {"node_id": int(record_id)})
        return True

