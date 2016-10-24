from abc import ABC, abstractmethod
from collections import namedtuple

import logging

log = logging.getLogger(__name__).addHandler(logging.NullHandler())

METADATA_PARENT_ID = "parent_id"
METADATA_KEY_PROV_TYPE = "prov_type"
METADATA_KEY_IDENTIFIER = "identifier"
METADATA_KEY_NAMESPACES = "namespaces"
METADATA_KEY_TYPE_MAP = "type_map"

# Return types for adapter classes
DbDocument = namedtuple("DbDocument", "document, bundles")
DbBundle = namedtuple("DbBundle", "records, bundle_record")

DbRecord = namedtuple("DbRecord", "attributes, metadata")
DbRelation = namedtuple("DbRelation", "attributes, metadata")


class BaseAdapter(ABC):
    """
    Interface class for a prov database adapter
    """

    @abstractmethod
    def __init__(self, *args, **kwargs):
        pass

    @abstractmethod
    def connect(self, authentication_info):
        """
        Establish the database connection / login into the database
        :param authentication_info: a custom dict with credentials
        :return: bool
        """
        pass

    @abstractmethod
    def save_document(self):
        """
        Create a new document id, so you only need to return a unique id
        :return: unique id as string
        """
        pass

    @abstractmethod
    def save_bundle(self, document_id, attributes, metadata):
        """
        Creates a bundle from the given parameter
        :param document_id: the parent document id
        :param attributes: The custom attributes for the bundle, normally empty
        :param metadata: The metadata for the bundle like the identifier
        :return: unique bundle id as string
        """
        pass

    @abstractmethod
    def save_record(self, bundle_id, attributes, metadata):
        """
        Creates a database node
        :param bundle_id: The new record belongs to this bundle
        :param attributes: Attributes as dict for the record. Be careful you have to encode the dict
        :param metadata: Metadata as dict for the record. Be careful you have to encode the dict but you can be sure that all meta keys are always there
        :return: Record id as string
        """
        pass

    @abstractmethod
    def save_relation(self, from_bundle_id, from_node, to_bundle_id, to_node, attributes, metadata):
        """
        Create a relation between 2 nodes
        :param from_bundle_id: The database for the from node
        :param from_node: The identifier
        :param to_bundle_id: The database id for the to node
        :param to_node: The identifier for the destination node
        :param attributes:  Attributes as dict for the record. Be careful you have to encode the dict
        :param metadata: Metadata as dict for the record. Be careful you have to encode the dict but you can be sure that all meta keys are always there
        :return:
        """
        pass

    @abstractmethod
    def get_document(self, document_id):
        """
        Returns a DbDocument named tuple
        :param document_id:
        :return: DbDocument: A namedtuple of the type DbDocument
        """
        pass

    @abstractmethod
    def get_bundle(self, bundle_id):
        """
        Get all bundle records from the database and return a DbBundle record
        :param bundle_id: The bundle id as str
        :return: DbBundle: A namedtuple of the type DbBundle
        """
        pass

    @abstractmethod
    def get_record(self, record_id):
        """
        Return a single record
        :param record_id:
        :return: DbRecord
        """
        pass

    @abstractmethod
    def get_relation(self, relation_id):
        """
        Returns a single relation
        :param relation_id:
        :return:DbRelation
        """
        pass

    @abstractmethod
    def delete_document(self, document_id):
        """
        Deletes a complete document with all included bundles
        :param document_id:
        :return: bool
        :raise NotFoundException
        """
        pass

    @abstractmethod
    def delete_bundle(self, bundle_id):
        """
        Delete a complete bundle
        :param bundle_id:
        :return:bool
        :raise NotFoundException
        """
        pass

    @abstractmethod
    def delete_record(self, record_id):
        """
        Delete a single record
        :param record_id:
        :return:bool
        :raise NotFoundException
        """
        pass

    @abstractmethod
    def delete_relation(self, relation_id):
        """
        Delete a single relation
        :param relation_id:
        :return:bool
        :raise NotFoundException
        """
        pass
