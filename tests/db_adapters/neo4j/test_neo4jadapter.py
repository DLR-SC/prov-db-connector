import unittest
from provdbconnector.db_adapters import Neo4jAdapter, NEO4J_USER,NEO4J_PASS,NEO4J_HOST, NEO4J_BOLT_PORT, NEO4J_HTTP_PORT
from provdbconnector.provapi import ProvApi
from provdbconnector.db_adapters import InvalidOptionsException, AuthException
from tests.db_adapters.test_baseadapter import AdapterTestTemplate
from tests.test_provapi import ProvApiTestTemplate


class Neo4jAdapterTests(AdapterTestTemplate):

    def setUp(self):
        self.instance = Neo4jAdapter()
        authInfo = {"user_name": NEO4J_USER,
                    "user_password": NEO4J_PASS,
                    "host": NEO4J_HOST+":"+NEO4J_BOLT_PORT
        }
        self.instance.connect(authInfo)
    @unittest.skip("Skiped because the server configuration currently is set to 'no password', so the authentication will never fail")
    def test_connect_fails(self):
        authInfo = {"user_name": NEO4J_USER,
                    "user_password": 'xxxxxx',
                    "host": NEO4J_HOST+":"+NEO4J_BOLT_PORT
        }
        self.instance.connect(authInfo)
        with self.assertRaises(AuthException):
            self.instance.connect(authInfo)

    def test_connect_invalid_options(self):
        authInfo = {"u": NEO4J_USER,
                    "p": 'xxxxxx',
                    "h": NEO4J_HOST+":"+NEO4J_BOLT_PORT
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


class Neo4jAdapterProvApiTests(ProvApiTestTemplate):

    def setUp(self):
        self.authInfo = {"user_name": NEO4J_USER,
                    "user_password": NEO4J_PASS,
                    "host": NEO4J_HOST + ":" + NEO4J_BOLT_PORT
                    }
        self.provapi = ProvApi(id=1, adapter=Neo4jAdapter, authinfo=self.authInfo)

    def tearDown(self):
        session = self.provapi._adapter._create_session()
        session.run("MATCH (x) DETACH DELETE x")
        del self.provapi