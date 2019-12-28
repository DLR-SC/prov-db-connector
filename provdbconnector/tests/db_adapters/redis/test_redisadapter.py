from provdbconnector.db_adapters.redis.redisadapter import RedisAdapter, REDIS_HOST

from provdbconnector.prov_db import ProvDb
from provdbconnector.tests import AdapterTestTemplate
from provdbconnector.tests import ProvDbTestTemplate


class RedisAdapterTests(AdapterTestTemplate):
    """
    This test extends from AdapterTestTemplate and provide a common set for the neo4j adapter
    """
    def setUp(self):
        """
        Setup the test
        """
        self.instance = RedisAdapter()
        auth_info = {
                    # TODO: Proper username / password / port config
                    # "user_name": REDIS_USER,
                    #  "user_password": NEO4J_PASS,
                     "host": REDIS_HOST
                     }
        self.instance.connect(auth_info)
        session = self.instance._create_session()
        session.delete()


    def tearDown(self):
        """
        Delete all data on the database
        :return:
        """
        session = self.instance._create_session()
        session.run("MATCH (x) DELETE x")
        del self.instance


class RedisAdapterProvDbTests(ProvDbTestTemplate):
    """
    High level api test for the neo4j adapter
    """
    def setUp(self):
        self.auth_info = {
                    # TODO: Proper username / password / port config
                    # "user_name": REDIS_USER,
                    #  "user_password": NEO4J_PASS,
                     "host": REDIS_HOST
                     }
        self.provapi = ProvDb(api_id=1, adapter=RedisAdapter, auth_info=self.auth_info)

    def clear_database(self):
        """
        This function get called before each test starts

        """
        session = self.provapi._adapter._create_session()
        params = {'prefix:purpose': "pleasure"}
        query = """MATCH (p:person)-[v:visited {purpose:$purpose}]->(c:country)
        		   RETURN p.name, p.age, v.purpose, c.name"""

        result = session.query(query, params)
        result.pretty_print()

        try:
            session.delete()
        except:
            # TODO Proper error handling
            print("Error but ignored")

    def tearDown(self):
        """
        Delete all data in the database
        """
        session = self.provapi._adapter._create_session()
        try:
            session.delete()
        except:
            # TODO Proper error handling
            print("Error but ignored")
        del self.provapi
