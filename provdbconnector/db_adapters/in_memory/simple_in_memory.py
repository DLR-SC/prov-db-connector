from provdbconnector.db_adapters.baseadapter import BaseAdapter, DbDocument, DbBundle, DbRecord, DbRelation, METADATA_KEY_IDENTIFIER, METADATA_KEY_PROV_TYPE
from provdbconnector.exceptions.database import InvalidOptionsException, NotFoundException
from provdbconnector.utils.serializer import encode_dict_values_to_primitive,split_into_formal_and_other_attributes,merge_record
from prov.constants import PROV_ASSOCIATION, PROV_TYPE, PROV_MENTION
from uuid import uuid4

import logging

log = logging.getLogger(__name__)


class SimpleInMemoryAdapter(BaseAdapter):

    document_bundle_ids = dict()

    bundles = dict()  # dict for all bundles including record and relation information

    all_nodes = dict()  # separate dict for records only (to get them by id)

    all_relations = dict()

    all_relations_id_map = dict()

    def __init__(self, *args):
        super(SimpleInMemoryAdapter, self).__init__()
        pass

    def connect(self, authentication_info):
        if authentication_info is not None:
            raise InvalidOptionsException()

        return True

    def save_record(self, attributes, metadata):
        #because it is in memory, we should copy the dicts to prevent others from modify the data
        attributes = attributes.copy()
        metadata = metadata.copy()

        # save all record information and return record id as string

        identifier = metadata[METADATA_KEY_IDENTIFIER]
        if str(identifier)in self.all_nodes:
            #try to merge nodes
            (old_attributes,old_metadata) = self.all_nodes[str(identifier)]
            (merged_attributes,merged_metadata) = merge_record(old_attributes,old_metadata, attributes,metadata)

            self.all_nodes.update({str(identifier): (merged_attributes,merged_metadata)})

        else:

            #encode your variables, based on your database architecture
            # (in this case it is not really necessary but for demonstration propose I saved the encoded vars )
            #attr = encode_dict_values_to_primitive(attributes)
            #meta = encode_dict_values_to_primitive(metadata)

            self.all_nodes.update({str(identifier): (attributes,metadata)})

        return str(identifier)

    def save_relation(self,  from_node,  to_node, attributes, metadata):
        # save all relation information and return the relation id as string

        # because it is in memory, we should copy the dicts to prevent others from modify the data
        attributes = attributes.copy()
        metadata = metadata.copy()

        #add dict if it is the first relation
        if str(from_node) not in self.all_relations:
            self.all_relations.update({str(from_node): dict()})


        # ===============
        # MERGE RELATION
        # ===============
        new_relation_formal_attributes = split_into_formal_and_other_attributes(attributes, metadata)

        #check that the from node already has some relations
        if str(from_node) in self.all_relations:

            #for each relation with the origin "from_node"
            for (relation_id,(to_identifier,old_attributes,old_metadata)) in self.all_relations[str(from_node)].items():
                #check if connecton is to the same identifier
                if str(to_node) == to_identifier and metadata[METADATA_KEY_PROV_TYPE] == old_metadata[METADATA_KEY_PROV_TYPE]:
                    # okay got potential duplicate... lets check the formal attributes
                    old_relation_formal_attributes = split_into_formal_and_other_attributes(old_attributes, old_metadata)

                    if old_relation_formal_attributes.formal == new_relation_formal_attributes.formal:
                        # got duplicate
                        (merged_attributes, merged_metadata) = merge_record(old_attributes,old_metadata,attributes,metadata)
                        self.all_relations[str(from_node)].update({relation_id: (to_identifier,merged_attributes,merged_metadata)})
                        return relation_id


        # ===============
        # CREATRE NEW RELATION
        # ===============

        id = str(uuid4())

        relations = self.all_relations[str(from_node)]
        relations.update({id: (str(to_node), attributes,metadata)})

        #self.all_records.update({new_rel_id: db_relations})

        return id


    def get_record(self, record_id):
        if record_id not in self.all_nodes:
            raise NotFoundException()

        (attributes, metadata)= self.all_nodes.get(record_id)
        attributes = encode_dict_values_to_primitive(attributes)
        metadata = encode_dict_values_to_primitive(metadata)
        db_record = DbRecord(attributes, metadata)

        return db_record

    def get_relation(self, relation_id):

        for (from_uri, relations) in self.all_relations.items():
            if relation_id in relations:

                (to_uri,  attributes, metadata) = relations[relation_id]

                attributes = encode_dict_values_to_primitive(attributes)
                metadata = encode_dict_values_to_primitive(metadata)

                return DbRelation(attributes, metadata)
        raise NotFoundException("could't find the relation with id {}".format(relation_id))

    def get_bundle(self, bundle_id):
        bundle = self.bundles.get(bundle_id)
        if bundle is None:
            raise NotFoundException()

        records = list()
        for record_id in bundle.records:
            record = self.all_records.get(record_id)
            records.append(record)

        return DbBundle(records, bundle.bundle_record)

    def delete_relation(self, relation_id):

        for (from_key, relations) in self.all_relations.items():
            if relation_id in relations:
                del relations[relation_id]
                break

        return True


    def delete_record(self, record_id):


        if record_id not in self.all_nodes:
            raise NotFoundException()

        del self.all_nodes[record_id]

        return True


    def get_records_tail(self, properties_dict=dict(), metadata_dict=dict(), depth=None):
        #only transform dict into list
        return list(self._get_records_tail_internal(properties_dict,metadata_dict).values())

    def _get_records_tail_internal(self, properties_dict=dict(), metadata_dict=dict(), max_depth=None, current_depth=0, result_records = None):
        origin_records = self.get_records_by_filter(properties_dict, metadata_dict)

        if result_records is None:
            result_records = dict()
        if current_depth == max_depth:
            return dict()
        for record in origin_records:
            from_identifier = record.metadata[METADATA_KEY_IDENTIFIER]
            if from_identifier  in self.all_relations:
                for (relation_id,(to_identifier, attributes, metadata)) in self.all_relations[from_identifier].items():
                    #find to node
                    if to_identifier not in result_records:
                        (to_attributes, to_metadata)= self.all_nodes[to_identifier]
                        result_records.update({to_identifier: DbRecord(to_attributes, to_metadata)})

                        # add other nodes recursive
                        result_records.update(self._get_records_tail_internal(metadata_dict={METADATA_KEY_IDENTIFIER: to_identifier},current_depth=current_depth+1,result_records=result_records))

                    # add relation to result
                    result_records.update({relation_id: DbRelation(attributes, metadata)})

        return result_records


    def delete_records_by_filter(self, properties_dict=dict(), metadata_dict=dict()):
        #erase all if no filter set
        if len(properties_dict) == 0 and len(metadata_dict) == 0:
            del self.all_nodes
            self.all_nodes = dict()
            return True

        #erase only matching nodes
        records_to_delete = self.get_records_by_filter(properties_dict,metadata_dict)

        for record in records_to_delete:
            if not isinstance(record, DbRecord):
                continue
            identifier = record.metadata[METADATA_KEY_IDENTIFIER]

            if identifier not in self.all_nodes:
                raise  NotFoundException("We cant find the id ")
            del self.all_nodes[identifier]

        return True


    def get_bundle_records(self, bundle_identifier):

        bundle_records = dict()

        #get all nodes for the bundle
        for (from_identifier, relations) in self.all_relations.items():
            # search in all relations for a relation that points to the bundle and also fulfill the other constraints
            for (relation_id, (to_identifier,attributes,metadata)) in relations.items():

                if to_identifier == str(bundle_identifier):
                    #got potential bundle association, check prov:type to be sure

                    if metadata[METADATA_KEY_PROV_TYPE] == PROV_ASSOCIATION and str(attributes[PROV_TYPE]) == "prov:bundleAssociation":
                        (attributes, metadata) = self.all_nodes[from_identifier]
                        bundle_records.update({from_identifier: DbRecord(attributes,metadata)})

        #search for all relations between the bundle nodes
        for from_identifier in bundle_records.copy().keys():
            if from_identifier in self.all_relations:
                for (relation_id,  (to_identifier, attributes, metadata)) in self.all_relations[from_identifier].items():

                    # If the target of the relation is also in the bundle the relation belongs to the bundle
                    if to_identifier in bundle_records:
                        bundle_records.update({relation_id: DbRelation(attributes, metadata)})

                    elif metadata[METADATA_KEY_PROV_TYPE] == PROV_MENTION:
                        # prov mentions used to connect between bundles , see w3c bundle links
                            bundle_records.update({relation_id: DbRelation(attributes, metadata)})

        return list(bundle_records.values())


    def _check_attribute_metadata_filter(self, properties_filter, metadata_filter, attributes,metadata):

        match = True

        meta_encoded = encode_dict_values_to_primitive(metadata)
        attr_encoded = encode_dict_values_to_primitive(attributes)

        filter_meta_encoded = encode_dict_values_to_primitive(metadata_filter)
        filter_attr_encoded = encode_dict_values_to_primitive(properties_filter)

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


    def get_records_by_filter(self, properties_dict=dict(), metadata_dict=dict()):

        return_records = list()
        return_keys = set()
        properties_filter_dict = encode_dict_values_to_primitive(properties_dict.copy())
        metadata_filter_dict = encode_dict_values_to_primitive(metadata_dict.copy())
        for (identifier,(attributes, metadata)) in self.all_nodes.items():



            if self._check_attribute_metadata_filter(properties_filter=properties_dict,
                                                     metadata_filter=metadata_dict,
                                                     metadata=metadata,
                                                     attributes=attributes):

                meta_encoded = encode_dict_values_to_primitive(metadata)
                attr_encoded = encode_dict_values_to_primitive(attributes)

                return_records.append(DbRecord(attr_encoded,meta_encoded))
                return_keys.add(identifier)

            else:
                #not match so dont add
                pass

        #get relations
        for (from_id,relations) in self.all_relations.items():

            if from_id in return_keys:

                for (relation_id, (to_id, attributes, metadata)) in relations.items():


                    attributes = encode_dict_values_to_primitive(attributes)
                    metadata = encode_dict_values_to_primitive(metadata)

                    return_records.append(DbRelation(attributes,metadata))

        return  return_records