from abc import ABC, abstractmethod
from collections import namedtuple

import logging
log = logging.getLogger(__name__).addHandler(logging.NullHandler())


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


class NotFoundException(DatabaseException):
    pass


METADATA_PARENT_ID = "parent_id"
METADATA_KEY_PROV_TYPE = "prov_type"
METADATA_KEY_IDENTIFIER = "identifier"
METADATA_KEY_NAMESPACES = "namespaces"
METADATA_KEY_TYPE_MAP = "type_map"


DbDocument = namedtuple("DbDocument", "records, bundles")
DbBundle = namedtuple("DbBundle",  "records, bundle_record")

DbRecord = namedtuple("DbRecord", "attributes, metadata")
DbRelation = namedtuple("DbRelation", "attributes, metadata")


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
    def create_relation(self, from_bundle_id, from_node, to_bundle_id, to_node, attributes, metadata):
        pass

    @abstractmethod
    def get_document(self, document_id):
        pass

    @abstractmethod
    def get_bundle(self, bundle_id):
        pass

    @abstractmethod
    def get_record(self, record_id):
        pass

    @abstractmethod
    def get_relation(self, relation_id):
        pass

    @abstractmethod
    def delete_document(self, document_id):
        pass

    @abstractmethod
    def delete_bundle(self, bundle_id):
        pass

    @abstractmethod
    def delete_record(self, record_id):
        pass

    @abstractmethod
    def delete_relation(self, relation_id):
        pass