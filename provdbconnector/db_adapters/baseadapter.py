import logging
from collections import namedtuple

log = logging.getLogger(__name__).addHandler(logging.NullHandler())

METADATA_PARENT_ID = "parent_id"
METADATA_KEY_PROV_TYPE = "prov_type"
METADATA_KEY_IDENTIFIER = "identifier"
METADATA_KEY_IDENTIFIER_ORIGINAL = "identifier_original"
METADATA_KEY_NAMESPACES = "namespaces"
METADATA_KEY_TYPE_MAP = "type_map"

# Return types for adapter classes
DbDocument = namedtuple("DbDocument", "document, bundles")
DbBundle = namedtuple("DbBundle", "records, bundle_record")

DbRecord = namedtuple("DbRecord", "attributes, metadata")
DbRelation = namedtuple("DbRelation", "attributes, metadata")


class BaseAdapter():
    """
    Interface class for a prov database adapter
    """

    def __init__(self, *args, **kwargs):
        pass

    def connect(self, authentication_info):
        """
        Establish the database connection / login into the database

        :param authentication_info: a custom dict with credentials
        :type authentication_info: dict
        :return: Indicate whether the connection was successful
        :rtype: boolean
        :raise InvalidOptionsException:
        """
        raise NotImplementedError("Abstract method")

    def save_element(self, attributes, metadata):
        """
        Saves a entity, activity or entity into the database

        :param attributes: Attributes as dict for the record. Be careful you have to encode the dict
        :type attributes: dict
        :param metadata: Metadata as dict for the record. Be careful you have to encode the dict but you can be sure that all meta keys are always there
        :type metadata: dict
        :return: Record id
        :rtype: str
        """
        raise NotImplementedError("Abstract method")

    def save_relation(self, from_node, to_node, attributes, metadata):
        """
        Create a relation between 2 nodes

        :param from_node: The identifier
        :type from_node: str
        :param to_node: The identifier for the destination node
        :type: to_node: str
        :param attributes:  Attributes as dict for the record. Be careful you have to encode the dict
        :type attributes: dict
        :param metadata: Metadata as dict for the record. Be careful you have to encode the dict but you can be sure that all meta keys are always there
        :type metadata: dict
        :return: Record id
        :rtype: str
        """
        raise NotImplementedError("Abstract method")

    def get_records_by_filter(self, attributes_dict=None, metadata_dict=None):
        """
        Returns all records (nodes and relations) based on a filter dict.
        The filter dict's are and AND combination but only the start node must fulfill the conditions.
        The result should contain all associated relations and nodes together

        :param attributes_dict:
        :type attributes_dict: dict
        :param metadata_dict:
        :type metadata_dict: dict
        :return: list of relations and nodes
        :rtype: list
        """
        raise NotImplementedError("Abstract method")

    def get_records_tail(self, attributes_dict=None, metadata_dict=None, depth=None):
        """
        Returns all connected nodes and relations based on a filter.
        The filter is an AND combination and this describes the filter only for the origin nodes.

        :param attributes_dict:
        :type attributes_dict: dict
        :param metadata_dict:
        :type metadata_dict: dict
        :param depth:
        :type depth: int
        :return: a list of relations and nodes
        :rtype: list
        """
        raise NotImplementedError("Abstract method")

    def get_bundle_records(self, bundle_identifier):
        """
        Returns the relations and nodes for a specific bundle identifier.
        Please use the bundle association to get all bundle nodes.
        Only the relations belongs to the bundle where the start AND end node belong also to the bundle.
        Except the prov:Mention see: W3C bundle links

        :param bundle_identifier: The bundle identifier
        :type bundle_identifier: str
        :return: list of nodes and bundles
        :rtype: list
        """
        raise NotImplementedError("Abstract method")

    def get_record(self, record_id):
        """
        Return a single record

        :param record_id: The id
        :type record_id: str
        :return: DbRecord
        :rtype: DbRecord
        """
        raise NotImplementedError("Abstract method")

    def get_relation(self, relation_id):
        """
        Returns a single relation

        :param relation_id: The id
        :type relation_id: str
        :return: DbRelation
        :rtype: DbRelation
        """
        raise NotImplementedError("Abstract method")

    def delete_records_by_filter(self, attributes_dict, metadata_dict):
        """
        Delete records by filter

        :param attributes_dict:
        :type attributes_dict: dict
        :param metadata_dict:
        :type metadata_dict: dict
        :return: Indicates whether the deletion was successful
        :rtype: boolean
        :raise NotFoundException:
        """
        raise NotImplementedError("Abstract method")

    def delete_record(self, record_id):
        """
        Delete a single record

        :param record_id:
        :type record_id: str
        :return: Indicates whether the deletion was successful
        :rtype: boolean
        :raise NotFoundException:
        """
        raise NotImplementedError("Abstract method")

    def delete_relation(self, relation_id):
        """
        Delete a single relation

        :param relation_id:
        :type relation_id: str
        :return: Indicates whether the deletion was successful
        :rtype: boolean
        :raise NotFoundException:
        """
        raise NotImplementedError("Abstract method")
