import unittest
from provdbconnector.db_adapters import SimpleInMemoryAdapter
from provdbconnector.provapi import ProvApi
from provdbconnector.db_adapters import InvalidOptionsException, AuthException
from tests.db_adapters.test_baseadapter import AdapterTestTemplate
from tests.test_provapi import ProvApiTestTemplate


# class SimpleInMemoryAdapterTest(AdapterTestTemplate):
#
#     def setUp(self):
#         self.instance = SimpleInMemoryAdapter()
#         self.instance.connect(None)
#
#     def test_connect_invalid_options(self):
#         authInfo = {"invalid": "Invalid"}
#         with self.assertRaises(InvalidOptionsException):
#             self.instance.connect(authInfo)
#
#     def tearDown(self):
#         del self.instance


# class Neo4jAdapterProvApiTests(ProvApiTestTemplate):
#
#     def setUp(self):
#         self.provapi = ProvApi(id=1, adapter=Neo4jAdapter, authinfo=None)
#
#     def tearDown(self):
#         del self.provapi