from provdbconnector.databases.baseconnector import BaseConnector, InvalidOptionsException, AuthException, DatabaseException,CreateNodeException

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

        prefixed_metadata = dict()
        prefixed_metadata["meta:{}".format("bundle_id")] = bundle_id

        for key, value in metadata.items():
            prefixed_metadata["meta:{}".format(key)] = value

        db_attributes  = attributes.copy()
        db_attributes.update(prefixed_metadata)


        provType = "Entry"

        # transform values
        for key, value in db_attributes.items():
            db_attributes[key] = encode_string_value_to_premitive(value)

        def createPropertyString(key):
            return "`%s`: {`%s`}"%(key,key)

        db_attributes_labels = map(lambda key:"`%s`: {`%s`}"%(key,key),list(db_attributes.keys()))


        str_val = ",".join(db_attributes_labels)

        session = self._create_session()

        result = session.run(NEO4J_CREATE_NODE_RETURN_ID % (provType,str_val),dict(db_attributes))

        id = None
        for record in result:
            id = record["ID"]

        if id is None:
            raise CreateNodeException("No ID property returned by database")

        return str(id)
