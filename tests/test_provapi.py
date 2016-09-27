import os
import unittest
from uuid import UUID

from provdbconnector import ProvApi
from provdbconnector.databases import InvalidOptionsException
from provdbconnector.databases import Neo4jAdapter, NEO4J_USER, NEO4J_PASS, NEO4J_HOST, NEO4J_BOLT_PORT, NEO4J_HTTP_PORT
from provdbconnector.provapi import NoDataBaseAdapter


class ProvApiTests(unittest.TestCase):

    def setUp(self):
        self.authInfo = {"user_name": NEO4J_USER,
                         "user_password": NEO4J_PASS,
                         "host": NEO4J_HOST+":"+NEO4J_BOLT_PORT
        }
        self.provapi = ProvApi(id=1, adapter=Neo4jAdapter, authinfo=self.authInfo)

    def tearDown(self):
        pass

    def test_provapi_instance(self):
        self.assertRaises(NoDataBaseAdapter, lambda: ProvApi())
        self.assertRaises(InvalidOptionsException, lambda: ProvApi(id=1, adapter=Neo4jAdapter))

        obj = ProvApi(id=1, adapter=Neo4jAdapter, authinfo=self.authInfo)
        self.assertIsInstance(obj, ProvApi)
        self.assertEquals(obj.apiid, 1)

        obj = ProvApi(adapter=Neo4jAdapter, authinfo=self.authInfo)
        self.assertIsInstance(obj.apiid,UUID)

    def test_create_document_from_json(self):
        self.provapi.create_document_from_json()

    def test_get_document_as_json(self):
        self.provapi.get_document_as_json()

    def test_create_document_from_prov(self):
        self.provapi.create_document_from_prov()

    def test_get_document_as_prov(self):
        self.provapi.get_document_as_prov()

    def test_create_document_from_xml(self):
        raise NotImplementedError()

    def test_get_document_as_xml(self):
        raise NotImplementedError()

    def test_create_document_from_provn(self):
        raise NotImplementedError()

    def test_get_document_as_provn(self):
        raise NotImplementedError()