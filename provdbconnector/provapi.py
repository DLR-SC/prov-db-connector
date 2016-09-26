from provdbconnector.databases import Neo4jAdapter


class ProvApi(object):

    def __init__(self, connector_type='Neo4j', *args):
        if connector_type == 'Neo4j':
            self._connector = Neo4jAdapter(*args)
        raise NotImplementedError
