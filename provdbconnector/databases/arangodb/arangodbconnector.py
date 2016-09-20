from provdbconnector.databases.baseconnector import BaseConnector


class ArangoDBConnector(BaseConnector):
    def __init__(self,*args):
        super(ArangoDBConnector, self).__init__()
        pass

    def connect(self):
        raise NotImplemented()

