from abc import ABC, abstractmethod

class ConnectorException (Exception):
    pass

class InvalidOptionsException(ConnectorException):
    pass

class AuthException(ConnectorException):
    pass

class DatabaseException(ConnectorException):
    pass

class CreateNodeException(DatabaseException):
    pass


METADATA_KEY_BUNDLE_ID  = "bundle_id"
METADATA_KEY_PROV_TYPE  = "prov_type"
METADATA_KEY_LABEL      = "label"
METADATA_KEY_NAMESPACES = "namespaces"
METADATA_KEY_TYPE_MAP   = "type_map"

class BaseConnector(ABC):

    @abstractmethod
    def __init__(self, *args, **kwargs):
        pass

    @abstractmethod
    def connect(self, authentication_info):
        pass

    @abstractmethod
    def create_document(self):
        pass

    @abstractmethod
    def create_bundle(self, document_id,attributes, metadata):
        pass


    @abstractmethod
    def create_record(self, bundle_id, attributes, metadata):
        pass
