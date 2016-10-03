from provdbconnector.db_adapters import BaseAdapter


class ArangoDBAdapter(BaseAdapter):
    def __init__(self, *args):
        super(ArangoDBAdapter, self).__init__()

    def connect(self, authentication_info):
        raise NotImplemented()

    def create_record(self, bundle_id, attributes, metadata):
        raise NotImplemented()

    def create_document(self):
        raise NotImplemented()

    def create_bundle(self, document_id, attributes, metadata):
        raise NotImplemented()

    def create_relation(self, from_bundle_id, from_node, to_bundle_id,to_node, attributes, metadata):
        raise NotImplemented()
