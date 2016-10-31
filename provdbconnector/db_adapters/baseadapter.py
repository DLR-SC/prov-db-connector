from abc import ABC, abstractmethod
from collections import namedtuple
from enum import Enum

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
    def save_record(self, attributes, metadata):
        """
        Creates a database node
        :param attributes: Attributes as dict for the record. Be careful you have to encode the dict
        :param metadata: Metadata as dict for the record. Be careful you have to encode the dict but you can be sure that all meta keys are always there
        :return: Record id as string
        """
        pass

    @abstractmethod
    def save_relation(self,  from_node,  to_node, attributes, metadata):
        """
        Create a relation between 2 nodes
        :param from_node: The identifier
        :param to_node: The identifier for the destionation node
        :param to_bundle_id: The database id for the to node
        :param to_node: The identifier for the destination node
        :param attributes:  Attributes as dict for the record. Be careful you have to encode the dict
        :param metadata: Metadata as dict for the record. Be careful you have to encode the dict but you can be sure that all meta keys are always there
        :return:
        """
        pass


    @abstractmethod
    def get_records_by_filter(self, properties_dict=None, metadata_dict=None):
        """
        Returns all records (nodes and relations) based on a filter dict.
        The filter dict's are and AND combination but only the start node must fulfill the conditions.
        The result should contain all associated relations and nodes togehter
        :param properties_dict:
        :param metadata_dict:
        :return:list of relations and nodes
        """
        pass

    @abstractmethod
    def get_records_tail(self,properties_dict=None, metadata_dict=None, depth=None):
        """
        Returns all connected nodes and relations based on a filter.
        The filter is an AND combination and this describes the filter only for the origin nodes.
        :param properties_dict:
        :param metadata_dict:
        :param depth:
        :return: a list of relations and nodes
        """
        pass

    @abstractmethod
    def get_bundle_records(self,bundle_identifier):
        """
        Returns the relations and nodes for a specific bundle identifier.
        Please use the bundle association to get all bundle nodes.
        Only the relations belongs to the bundle where the start AND end node belong also to the bundle.
        Except the prov:Mention see: W3C bundle links
        :param bundle_identifier:
        :return:list of nodes and bundles
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
    def delete_records_by_filter(self, properties_dict, metadata_dict):
        """
        Delte records by filter
        :param properties_dict:
        :param metadata_dict:
        :return:
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
