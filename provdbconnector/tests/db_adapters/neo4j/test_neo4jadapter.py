import unittest

from provdbconnector.exceptions.database import InvalidOptionsException, AuthException
from provdbconnector import Neo4jAdapter, NEO4J_USER, NEO4J_PASS, NEO4J_HOST, NEO4J_BOLT_PORT
from provdbconnector.prov_db import ProvDb
from provdbconnector.tests import AdapterTestTemplate
from provdbconnector.tests import ProvDbTestTemplate


class Neo4jAdapterTests(AdapterTestTemplate):
    """
    This test extends from AdapterTestTemplate and provide a common set for the neo4j adapter
    """
    def setUp(self):
        """
        Setup the test
        """
        self.instance = Neo4jAdapter()
        auth_info = {"user_name": NEO4J_USER,
                     "user_password": NEO4J_PASS,
                     "host": NEO4J_HOST + ":" + NEO4J_BOLT_PORT
                     }
        self.instance.connect(auth_info)
        session = self.instance._create_session()
        session.run("MATCH (x) DETACH DELETE x")

    @unittest.skip(
        "Skipped because the server configuration currently is set to 'no password', so the authentication will never fail")
    def test_connect_fails(self):
        """
        Try to connect with the wrong password
        """
        auth_info = {"user_name": NEO4J_USER,
                     "user_password": 'xxxxxx',
                     "host": NEO4J_HOST + ":" + NEO4J_BOLT_PORT
                     }
        self.instance.connect(auth_info)
        with self.assertRaises(AuthException):
            self.instance.connect(auth_info)

    def test_connect_invalid_options(self):
        """
        Try to connect with some invalid arguments
        """
        auth_info = {"u": NEO4J_USER,
                     "p": 'xxxxxx',
                     "h": NEO4J_HOST + ":" + NEO4J_BOLT_PORT
                     }
        with self.assertRaises(InvalidOptionsException):
            self.instance.connect(auth_info)

    def tearDown(self):
        """
        Delete all data on the database
        :return:
        """
        session = self.instance._create_session()
        session.run("MATCH (x) DETACH DELETE x")
        del self.instance


class Neo4jAdapterProvDbTests(ProvDbTestTemplate):
    """
    High level api test for the neo4j adapter
    """
    def setUp(self):
        self.auth_info = {"user_name": NEO4J_USER,
                          "user_password": NEO4J_PASS,
                          "host": NEO4J_HOST + ":" + NEO4J_BOLT_PORT
                          }
        self.provapi = ProvDb(api_id=1, adapter=Neo4jAdapter, auth_info=self.auth_info)

    def clear_database(self):
        """
        This function get called before each test starts

        """
        session = self.provapi._adapter._create_session()
        session.run("MATCH (x) DETACH DELETE x")

    def tearDown(self):
        """
        Delete all data in the database
        """
        session = self.provapi._adapter._create_session()
        session.run("MATCH (x) DETACH DELETE x")
        del self.provapi
