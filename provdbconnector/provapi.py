from provdbconnector.databases import Neo4jConnector


class ProvApi(object):

    def __init__(self, connector_type='Neo4j', *args):
        if connector_type == 'Neo4j':
            self._connector = Neo4jConnector(*args)
        raise NotImplementedError
