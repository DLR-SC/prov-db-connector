from abc import ABC, abstractmethod


class AdapterException(Exception):
    pass


class InvalidOptionsException(AdapterException):
    pass


class AuthException(AdapterException):
    pass


class DatabaseException(AdapterException):
    pass


class CreateRecordException(DatabaseException):
    pass


class CreateRelationException(DatabaseException):
    pass


METADATA_KEY_BUNDLE_ID = "bundle_id"
METADATA_KEY_PROV_TYPE = "prov_type"
METADATA_KEY_LABEL = "label"
METADATA_KEY_NAMESPACES = "namespaces"
METADATA_KEY_TYPE_MAP = "type_map"


class BaseAdapter(ABC):
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
    def create_bundle(self, document_id, attributes, metadata):
        pass

    @abstractmethod
    def create_record(self, bundle_id, attributes, metadata):
        pass

    @abstractmethod
    def create_relation(self, bundle_id, from_node, to_node, attributes, metadata):
        pass
