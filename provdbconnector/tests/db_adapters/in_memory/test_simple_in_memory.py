from provdbconnector.exceptions.database import InvalidOptionsException
from provdbconnector.db_adapters.in_memory import SimpleInMemoryAdapter
from provdbconnector.prov_db import ProvDb
from provdbconnector.tests import AdapterTestTemplate
from provdbconnector.tests import ProvDbTestTemplate


class SimpleInMemoryAdapterTest(AdapterTestTemplate):
    """
    This class implements the AdapterTestTemplate and only override some functions.

    """
    def setUp(self):
        """
        Connect to your database

        """
        self.instance = SimpleInMemoryAdapter()
        self.instance.connect(None)

    def test_connect_invalid_options(self):
        """
        Test your connect function with invalid data

        """
        auth_info = {"invalid": "Invalid"}
        with self.assertRaises(InvalidOptionsException):
            self.instance.connect(auth_info)

    def clear_database(self):
        """
        Clear the database

        """
        self.instance.all_nodes = dict()
        self.instance.all_relations= dict()

    def tearDown(self):
        """
        Delete your instance

        """
        del self.instance


class SimpleInMemoryAdapterProvDbTests(ProvDbTestTemplate):
    """
    This is the high level test for the SimpleInMemoryAdapter

    """
    def setUp(self):
        """
        Setup a ProvDb instance
        """
        self.provapi = ProvDb(api_id=1, adapter=SimpleInMemoryAdapter, auth_info=None)

    def clear_database(self):
        """
        Clear function get called before each test starts

        """
        self.provapi._adapter.all_nodes = dict()
        self.provapi._adapter.all_relations = dict()

    def tearDown(self):
        """
        Delete prov api instance
        """
        del self.provapi
