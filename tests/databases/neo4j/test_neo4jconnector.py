import unittest
from provdbconnector.databases import Neo4jConnector
from tests.databases.test_baseconnector import ConnectorTestTemplate

class Neo4jConnectorTests(ConnectorTestTemplate):

    def setUp(self):
        self.instance = Neo4jConnector()
        self.instance.connect()

    def tearDown(self):
        del self.instance

