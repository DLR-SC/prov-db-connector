import logging
from uuid import uuid4

from prov.constants import PROV_ASSOCIATION, PROV_TYPE, PROV_MENTION
from provdbconnector.db_adapters.baseadapter import BaseAdapter, DbRecord, DbRelation, METADATA_KEY_IDENTIFIER, \
    METADATA_KEY_PROV_TYPE
from provdbconnector.exceptions.database import InvalidOptionsException, NotFoundException
from provdbconnector.utils.serializer import encode_dict_values_to_primitive, split_into_formal_and_other_attributes, \
    merge_record

log = logging.getLogger(__name__)


class SimpleInMemoryAdapter(BaseAdapter):
    """
    The simple in memory adapter is a reference implementation for a database adapter to save prov information
    into a graph database


    For exmaple to use the simple db_adapter use the following script

    .. literalinclude:: ../examples/simple_example.py
            :linenos:
            :language: python

    """
    all_nodes = dict()  # separate dict for records only (to get them by id)
    """
    Contains all nodes
    """
    all_relations = dict()
    """
    Contains all relation according to the following structure
    `(start_identifier, (end_identifier,attributes, metadata))``
    """

    def __init__(self, *args):
        """
        Init the adapter without any params
        :param args:
        """
        super(SimpleInMemoryAdapter, self).__init__()
        pass

    def connect(self, authentication_info):
        """
        This function setups your database connection (auth / service discover)

        :param authentication_info: The info to connect to the db
        :type authentication_info: dict or None
        :return: The result of the connection attempt
        :rtype: Bool
        """

        if authentication_info is not None:
            raise InvalidOptionsException()

        return True

    def save_element(self, attributes, metadata):
        """
        Store a single node in the database and if necessary and possible merge the node

        :param attributes: The actual provenance data
        :type attributes: dict
        :param metadata: Some metadata that are not PROV-O related
        :type metadata: dict
        :return: id of the record
        :rtype: str
        """

        # because it is in memory, we should copy the dicts to prevent others from modify the data
        attributes = attributes.copy()
        metadata = metadata.copy()

        # save all record information and return record id as string

        identifier = metadata[METADATA_KEY_IDENTIFIER]
        if str(identifier) in self.all_nodes:
            # try to merge nodes
            (old_attributes, old_metadata) = self.all_nodes[str(identifier)]
            (merged_attributes, merged_metadata) = merge_record(old_attributes, old_metadata, attributes, metadata)

            self.all_nodes.update({str(identifier): (merged_attributes, merged_metadata)})

        else:

            # encode your variables, based on your database architecture
            # (in this case it is not really necessary but for demonstration propose I saved the encoded vars )
            # attr = encode_dict_values_to_primitive(attributes)
            # meta = encode_dict_values_to_primitive(metadata)

            self.all_nodes.update({str(identifier): (attributes, metadata)})

        return str(identifier)

    def save_relation(self, from_node, to_node, attributes, metadata):
        """
        Store a relation between 2 nodes in the database.
        Merge also the relation if necessary and possible

        :param from_node: The identifier for the start node
        :type from_node: prov.model.Identifier
        :param to_node: The identifier for the end node
        :type to_node: prov.model.Identifier
        :param attributes: The actual provenance data
        :type attributes: dict
        :param metadata: Some metadata that are not PROV-O related
        :type metadata: dict
        :return: The id of the relation
        :rtype: str
        """

        # save all relation information and return the relation id as string

        # because it is in memory, we should copy the dicts to prevent others from modify the data
        attributes = attributes.copy()
        metadata = metadata.copy()

        # add dict if it is the first relation
        if str(from_node) not in self.all_relations:
            self.all_relations.update({str(from_node): dict()})

        # ===============
        # MERGE RELATION
        # ===============
        new_relation_formal_attributes = split_into_formal_and_other_attributes(attributes, metadata)

        # check that the from node already has some relations
        if str(from_node) in self.all_relations:

            # for each relation with the origin "from_node"
            for (relation_id, (to_identifier, old_attributes, old_metadata)) in self.all_relations[
                str(from_node)].items():
                # check if connection is to the same identifier
                if str(to_node) == to_identifier and metadata[METADATA_KEY_PROV_TYPE] == old_metadata[
                    METADATA_KEY_PROV_TYPE]:
                    # okay got potential duplicate... lets check the formal attributes
                    old_relation_formal_attributes = split_into_formal_and_other_attributes(old_attributes,
                                                                                            old_metadata)

                    if old_relation_formal_attributes.formal == new_relation_formal_attributes.formal:
                        # got duplicate
                        (merged_attributes, merged_metadata) = merge_record(old_attributes, old_metadata, attributes,
                                                                            metadata)
                        self.all_relations[str(from_node)].update(
                            {relation_id: (to_identifier, merged_attributes, merged_metadata)})
                        return relation_id

        # ===============
        # CREATE NEW RELATION
        # ===============

        id = str(uuid4())

        relations = self.all_relations[str(from_node)]
        relations.update({id: (str(to_node), attributes, metadata)})

        return id

    def get_record(self, record_id):
        """
        Get a ProvDocument from the database based on the document id

        :param record_id: The id of the node
        :type record_id: str
        :return: A named tuple with (attributes, metadata)
        :rtype: DbRecord
        """
        if record_id not in self.all_nodes:
            raise NotFoundException()

        (attributes, metadata) = self.all_nodes.get(record_id)
        attributes = encode_dict_values_to_primitive(attributes)
        metadata = encode_dict_values_to_primitive(metadata)
        db_record = DbRecord(attributes, metadata)

        return db_record

    def get_relation(self, relation_id):
        """
        Return the relation behind the relation_id

        :param relation_id: The id of the relation
        :type relation_id: str
        :return: The namedtuple with (attributes, metadata)
        :rtype: DbRelation
        """

        for (from_uri, relations) in self.all_relations.items():
            if relation_id in relations:
                (to_uri, attributes, metadata) = relations[relation_id]

                attributes = encode_dict_values_to_primitive(attributes)
                metadata = encode_dict_values_to_primitive(metadata)

                return DbRelation(attributes, metadata)
        raise NotFoundException("could't find the relation with id {}".format(relation_id))

    def get_records_by_filter(self, attributes_dict=None, metadata_dict=None):
        """
        Filter all nodes based on the provided attributes and metadata dict
        The filter is currently defined as follows:

        - The filter is only applied to the start node
        - All connections from the start node are also included in the result set

        :param attributes_dict: A filter dict with a conjunction of all values in the attributes_dict and metadata_dict
        :type attributes_dict: dict
        :param metadata_dict: A filter for the metadata with a conjunction of all values (also in the attributes_dict )
        :type metadata_dict: dict
        :return: The list of matching relations and nodes
        :rtype: List(DbRecord or Dbrelation)
        """
        if attributes_dict is None:
            attributes_dict = dict()
        if metadata_dict is None:
            metadata_dict = dict()

        return_records = list()
        return_keys = set()
        properties_filter_dict = encode_dict_values_to_primitive(attributes_dict.copy())
        metadata_filter_dict = encode_dict_values_to_primitive(metadata_dict.copy())
        for (identifier, (attributes, metadata)) in self.all_nodes.items():

            if self._check_attribute_metadata_filter(attributes_filter=properties_filter_dict,
                                                     metadata_filter=metadata_filter_dict,
                                                     metadata=metadata,
                                                     attributes=attributes):

                meta_encoded = encode_dict_values_to_primitive(metadata)
                attr_encoded = encode_dict_values_to_primitive(attributes)

                return_records.append(DbRecord(attr_encoded, meta_encoded))
                return_keys.add(identifier)

            else:
                # not match so don't add
                pass

        # get relations
        for (from_id, relations) in self.all_relations.items():

            if from_id in return_keys:

                for (relation_id, (to_id, attributes, metadata)) in relations.items():
                    if to_id in return_keys:
                        attributes = encode_dict_values_to_primitive(attributes)
                        metadata = encode_dict_values_to_primitive(metadata)

                        return_records.append(DbRelation(attributes, metadata))

        return return_records

    def get_records_tail(self, attributes_dict=None, metadata_dict=None, depth=None):
        """
        Return the provenance based on a filter combination.
        The filter dicts are only relevant for the start nodes.
        They describe the params to get the start nodes (for example a filter for a specific identifier ) and from there
        we want all connected nodes

        :param attributes_dict: A filter dict with a conjunction of all values in the attributes_dict and metadata_dict
        :type attributes_dict: dict
        :param metadata_dict: A filter for the metadata with a conjunction of all values (also in the attributes_dict )
        :type metadata_dict: dict
        :param depth: The level of detail, default to infinite
        :type depth: int
        :return: A list of DbRelations and DbRecords
        :rtype: list(DbRelation or DbRecord)
        """
        # only transform dict into list
        return list(self._get_records_tail_internal(attributes_dict, metadata_dict).values())

    def _get_records_tail_internal(self, attributes_dict=None, metadata_dict=None, max_depth=None, current_depth=0,
                                   result_records=None):
        """
        Internal function, because we need to call it recursive. This function is only neccecary in this in memory adapter.
        It's your choice how you implement the get_records_tail function

        :param attributes_dict: A filter dict with a conjunction of all values in the attributes_dict and metadata_dict
        :type attributes_dict: dict
        :param metadata_dict: A filter for the metadata with a conjunction of all values (also in the attributes_dict )
        :type metadata_dict: dict
        :param max_depth:
        :param current_depth:
        :param result_records:
        :return:
        """

        if attributes_dict is None:
            attributes_dict = dict()
        if metadata_dict is None:
            metadata_dict = dict()

        origin_records = self.get_records_by_filter(attributes_dict, metadata_dict)

        if result_records is None:
            result_records = dict()
        if current_depth == max_depth:
            return dict()
        for record in origin_records:
            from_identifier = record.metadata[METADATA_KEY_IDENTIFIER]
            if from_identifier in self.all_relations:
                for (relation_id, (to_identifier, attributes, metadata)) in self.all_relations[from_identifier].items():
                    # find to node
                    if to_identifier not in result_records:
                        (to_attributes, to_metadata) = self.all_nodes[to_identifier]
                        result_records.update({to_identifier: DbRecord(to_attributes, to_metadata)})

                        # add other nodes recursive
                        result_records.update(
                            self._get_records_tail_internal(metadata_dict={METADATA_KEY_IDENTIFIER: to_identifier},
                                                            current_depth=current_depth + 1,
                                                            result_records=result_records))

                    # add relation to result
                    result_records.update({relation_id: DbRelation(attributes, metadata)})

        return result_records

    def get_bundle_records(self, bundle_identifier):
        """
        Get the records for a specific bundle identifier

        This include all nodes that have a relation of the prov:type = prov:bundleAssociation and also
        all relation where the start and end node are in the bundle.
        Also you should add the prov mentionOf relation where the start node is in the bundle.
        See https://www.w3.org/TR/prov-links/

        :param bundle_identifier: The identifier of the bundle
        :type bundle_identifier: prov.model.Identifier
        :return: The list with the bundle nodes and all connections where the start node and end node in the bundle.
        :rtype: list(DbRelation or DbRecord )
        """
        bundle_records = dict()

        # get all nodes for the bundle
        for (from_identifier, relations) in self.all_relations.items():
            # search in all relations for a relation that points to the bundle and also fulfill the other constraints
            for (relation_id, (to_identifier, attributes, metadata)) in relations.items():

                if to_identifier == str(bundle_identifier):
                    # got potential bundle association, check prov:type to be sure

                    if metadata[METADATA_KEY_PROV_TYPE] == PROV_ASSOCIATION and str(
                            attributes[PROV_TYPE]) == "prov:bundleAssociation":
                        (attributes, metadata) = self.all_nodes[from_identifier]
                        bundle_records.update({from_identifier: DbRecord(attributes, metadata)})

        # search for all relations between the bundle nodes
        for from_identifier in bundle_records.copy().keys():
            if from_identifier in self.all_relations:
                for (relation_id, (to_identifier, attributes, metadata)) in self.all_relations[from_identifier].items():

                    # If the target of the relation is also in the bundle the relation belongs to the bundle
                    if to_identifier in bundle_records:
                        bundle_records.update({relation_id: DbRelation(attributes, metadata)})

                    elif metadata[METADATA_KEY_PROV_TYPE] == PROV_MENTION:
                        # prov mentions used to connect between bundles , see w3c bundle links
                        bundle_records.update({relation_id: DbRelation(attributes, metadata)})

        return list(bundle_records.values())

    def delete_records_by_filter(self, attributes_dict=None, metadata_dict=None):
        """
        Delete a set of records based on filter conditions

        :param attributes_dict: A filter dict with a conjunction of all values in the attributes_dict and metadata_dict
        :type attributes_dict: dict
        :param metadata_dict: A filter for the metadata with a conjunction of all values (also in the attributes_dict )
        :type metadata_dict: dict
        :return: The result of the operation
        :rtype: Bool
        """
        if attributes_dict is None:
            attributes_dict = dict()
        if metadata_dict is None:
            metadata_dict = dict()

        # erase all if no filter set
        if len(attributes_dict) == 0 and len(metadata_dict) == 0:
            del self.all_nodes
            self.all_nodes = dict()
            return True

        # erase only matching nodes
        records_to_delete = self.get_records_by_filter(attributes_dict, metadata_dict)

        for record in records_to_delete:
            if not isinstance(record, DbRecord):
                continue
            identifier = record.metadata[METADATA_KEY_IDENTIFIER]

            if identifier not in self.all_nodes:
                raise NotFoundException("We cant find the id ")
            del self.all_nodes[identifier]

        return True

    def delete_record(self, record_id):
        """
        Delete a single record

        :param record_id: The node id
        :type record_id: str
        :return: Result of the delete operation
        :rtype: Bool
        """

        if record_id not in self.all_nodes:
            raise NotFoundException()

        del self.all_nodes[record_id]

        return True

    def delete_relation(self, relation_id):
        """
        Delete the relation

        :param relation_id: The relation id
        :type relation_id: str
        :return: Result of the delete operation
        :rtype: Bool
        """

        for (from_key, relations) in self.all_relations.items():
            if relation_id in relations:
                del relations[relation_id]
                break

        return True
    @staticmethod
    def _check_attribute_metadata_filter(attributes_filter, metadata_filter, attributes, metadata):
        """
        This function checks if a (attributes, metadata) match to the filter

        :param attributes_dict: A filter dict with a conjunction of all values in the attributes_dict and metadata_dict
        :type attributes_dict: dict
        :param metadata_dict: A filter for the metadata with a conjunction of all values (also in the attributes_dict )
        :type metadata_dict: dict
        :param attributes: The actual provenance data
        :type attributes: dict
        :param metadata: Some metadata that are not PROV-O related
        :type metadata: dict
        :return:
        """
        match = True

        meta_encoded = encode_dict_values_to_primitive(metadata)
        attr_encoded = encode_dict_values_to_primitive(attributes)

        filter_meta_encoded = encode_dict_values_to_primitive(metadata_filter)
        filter_attr_encoded = encode_dict_values_to_primitive(attributes_filter)

        # check properties
        for (key, value) in filter_attr_encoded.items():
            if key not in attr_encoded:
                return False

            elif attr_encoded[key] != value:
                return False

        # check metadata
        for (key, value) in filter_meta_encoded.items():
            if key not in meta_encoded:
                return False

            elif meta_encoded[key] != value:
                return False

        return True
