import os
import unittest
from provdbconnector.databases import Neo4jAdapter
from provdbconnector.databases import InvalidOptionsException, AuthException
from tests.databases.test_baseadapter import AdapterTestTemplate


neo4j_default_user = os.environ.get('NEO4J_USERNAME', 'neo4j')
neo4j_default_pass = os.environ.get('NEO4J_PASSWORD', 'neo4jneo4j')
neo4j_default_host = os.environ.get('NEO4J_HOST', 'localhost:7687')

class Neo4jAdapterTests(AdapterTestTemplate):

    def setUp(self):
        self.instance = Neo4jAdapter()
        authInfo = {"user_name": neo4j_default_user,
                    "user_password": neo4j_default_pass,
                    "host": neo4j_default_host
        }
        self.instance.connect(authInfo)

    def test_connect_fails(self):
        authInfo = {"user_name": neo4j_default_user,
                    "user_password": 'xxxxxx',
                    "host": neo4j_default_host
        }
        self.instance.connect(authInfo)
        with self.assertRaises(AuthException):
            self.instance.connect(authInfo)

    def test_connect_invalid_options(self):
        authInfo = {"u": neo4j_default_user,
                    "p": 'xxxxxx',
                    "h": neo4j_default_host
        }
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
        session.run("MATCH (x) DETACH DELETE x")
        del self.instance

