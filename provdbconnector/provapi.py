from provdbconnector.databases import Neo4jConnector

class ProvApi(object):

    def __init__(self, connector_type, *args, **kwargs):
        if connector_type == 'Neo4j':
            self.connector = Neo4jConnector(*args, **kwargs)
        pass
