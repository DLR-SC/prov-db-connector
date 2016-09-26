import unittest
from provdbconnector.databases import Neo4jConnector
from provdbconnector.databases.baseconnector import InvalidOptionsException,AuthException
from tests.databases.test_baseconnector import ConnectorTestTemplate

test_user_name = "neo4j"
test_user_pass = "neo4jneo4j"
test_host = "192.168.99.100:7687"

class Neo4jConnectorTests(ConnectorTestTemplate):

    def setUp(self):
        self.instance = Neo4jConnector()
        authInfo = dict()
        authInfo.update({"user_name": test_user_name})
        authInfo.update({"user_password":test_user_pass})
        authInfo.update({"host": test_host})

        self.instance.connect(authInfo)

    def test_connect(self):
        authInfo = dict()
        authInfo.update({"user_name": "neo4j"})
        authInfo.update({"user_password": "neo4jneo4j"})
        authInfo.update({"host": test_host})
        self.instance.connect(authInfo)

    def test_connect_fails(self):
        authInfo = dict()
        authInfo.update({"user_name": "neo4j"})
        authInfo.update({"user_password": "xxxxxxxxxx"})
        authInfo.update({"host": test_host})
        self.instance.connect(authInfo)
        with self.assertRaises(AuthException):
            self.instance.connect(authInfo)

    def test_connect_invalid_options(self):
        authInfo = dict()
        authInfo.update({"xxxx": "neo4j"})
        authInfo.update({"xx": "neo4jneo4j"})
        authInfo.update({"xx": test_host})

        with self.assertRaises(InvalidOptionsException):
            self.instance.connect(authInfo)

    def test_create_document_id_increment(self):
        first= self.instance.create_document()

        first= int(first)

        second = self.instance.create_document()
        second = int(second)

        self.assertEqual(first+1,second)

    def tearDown(self):
        session = self.instance._create_session()
        #session.run("MATCH (x) DETACH DELETE x")
        del self.instance

