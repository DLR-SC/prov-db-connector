from provdbconnector.databases import BaseAdapter


class ArangoDBAdapter(BaseAdapter):
    def __init__(self,*args):
        super(ArangoDBAdapter, self).__init__()
        pass

    def connect(self, authentication_info):
        raise NotImplemented()

