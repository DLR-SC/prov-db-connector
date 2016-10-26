from provdbconnector.exceptions.database import InvalidOptionsException
from provdbconnector.db_adapters.in_memory import SimpleInMemoryAdapter
from provdbconnector.provapi import ProvApi
from provdbconnector.tests import AdapterTestTemplate
from provdbconnector.tests import ProvApiTestTemplate


class SimpleInMemoryAdapterTest(AdapterTestTemplate):
    def setUp(self):
        self.instance = SimpleInMemoryAdapter()
        self.instance.connect(None)

    def test_connect_invalid_options(self):
        auth_info = {"invalid": "Invalid"}
        with self.assertRaises(InvalidOptionsException):
            self.instance.connect(auth_info)

    def clear_database(self):
        self.instance.all_nodes = dict()
        self.instance.all_relations= dict()
    def tearDown(self):
        del self.instance


class SimpleInMemoryAdapterProvApiTests(ProvApiTestTemplate):
    def setUp(self):
        self.provapi = ProvApi(api_id=1, adapter=SimpleInMemoryAdapter, auth_info=None)

    def tearDown(self):
        del self.provapi
