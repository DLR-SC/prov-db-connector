import unittest
from provdbconnector.databases import Neo4jConnector

class Neo4jConnectorTests(unittest.TestCase):

    def setUp(self):
        pass

    def test_connect(self):#
        neo4j = Neo4jConnector()
        neo4j.connect()

    def tearDown(self):
        pass

