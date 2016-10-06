from provdbconnector.db_adapters import InvalidOptionsException
from provdbconnector.db_adapters import SimpleInMemoryAdapter
from provdbconnector.provapi import ProvApi
from provdbconnector.tests import AdapterTestTemplate
from provdbconnector.tests import ProvApiTestTemplate


class SimpleInMemoryAdapterTest(AdapterTestTemplate):

    def setUp(self):
        self.instance = SimpleInMemoryAdapter()
        self.instance.connect(None)

    def test_connect_invalid_options(self):
        authInfo = {"invalid": "Invalid"}
        with self.assertRaises(InvalidOptionsException):
            self.instance.connect(authInfo)

    def tearDown(self):
        del self.instance


class SimpleInMemoryAdapterProvApiTests(ProvApiTestTemplate):

     def setUp(self):
         self.provapi = ProvApi(api_id=1, adapter=SimpleInMemoryAdapter, authinfo=None)

     def tearDown(self):
         del self.provapi