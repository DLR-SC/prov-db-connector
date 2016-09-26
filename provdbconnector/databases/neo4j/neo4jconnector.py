from provdbconnector.databases.baseconnector import BaseConnector, InvalidOptionsException, AuthException, DatabaseException,CreateRecordException,METADATA_KEY_PROV_TYPE, METADATA_KEY_BUNDLE_ID

from neo4j.v1.exceptions import ProtocolError
from neo4j.v1 import GraphDatabase, basic_auth
from provdbconnector.utils.serializer import encode_string_value_to_premitive

NEO4J_TEST_CONNECTION = """MATCH (n) RETURN count(n) as count"""

#create
NEO4J_CREATE_DOCUMENT_NODE_RETURN_ID = """CREATE (node { }) RETURN ID(node) as ID"""
NEO4J_CREATE_NODE_RETURN_ID = """CREATE (node:%s { %s}) RETURN ID(node) as ID """ #args: provType, values

#delete
NEO4J_DELETE__NODE_BY_ID = """MATCH  (x) Where ID(x) = {id} DETACH DELETE x """
class Neo4jConnector(BaseConnector):
    def __init__(self,*args):
        super(Neo4jConnector, self).__init__()
        self.driver = None
        pass

    def _create_session(self):
        session = self.driver.session()

        if not session.healthy:
            raise AuthException()
        return session


    def connect(self, authentication_options):

        user_name = authentication_options.get("user_name")
        user_pass = authentication_options.get("user_password")
        host = authentication_options.get("host")

        if user_name is None or user_pass is None or host is None:
            raise InvalidOptionsException

        try:
            self.driver = GraphDatabase.driver("bolt://{}".format(host), auth=basic_auth(user_name, user_pass))

        except ProtocolError as e:
            raise InvalidOptionsException(e)

        self._create_session()

    def _prefix_metadata(self, metadata):
        prefixed_metadata = dict()
        for key, value in metadata.items():
            prefixed_metadata["meta:{}".format(key)] = value

        return prefixed_metadata

    def _parse_to_primitive_attributes(self, attributes,prefixed_metadata):
        db_attributes = attributes.copy()
        db_attributes.update(prefixed_metadata)

        # transform values
        for key, value in db_attributes.items():
            db_attributes[key] = encode_string_value_to_premitive(value)
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

        result_delete = session.run(NEO4J_DELETE__NODE_BY_ID, {"id":id})

        if id is None:
            raise DatabaseException("Could not get a valid ID result back")

        return str(id+1)

    def create_bundle(self, document_id, attributes, metadata):

        return self.create_record(document_id,attributes,metadata)


    def create_record(self,bundle_id,attributes,metadata):

        metadata = metadata.copy()
        metadata.update({METADATA_KEY_BUNDLE_ID: bundle_id})

        prefixed_metadata = self._prefix_metadata(metadata)

        db_attributes = self._parse_to_primitive_attributes(attributes,prefixed_metadata)

        provType = metadata[METADATA_KEY_PROV_TYPE]

        label_str= self._get_attributes_labels_cypher_string(db_attributes)

        session = self._create_session()

        command = NEO4J_CREATE_NODE_RETURN_ID % (provType.localpart,label_str)
        result = session.run(command,dict(db_attributes))

        id = None
        for record in result:
            id = record["ID"]

        if id is None:
            raise CreateRecordException("No ID property returned by database for the command {}".format(command))

        return str(id)


