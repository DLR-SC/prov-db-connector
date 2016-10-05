from provdbconnector.db_adapters.baseadapter import  BaseAdapter, DbDocument,DbBundle,DbRecord,DbRelation, InvalidOptionsException, NotFoundException, METADATA_KEY_IDENTIFIER
from collections import  namedtuple
from provdbconnector.utils.serializer import encode_dict_values_to_primitive
from uuid import uuid4

class SimpleInMemoryAdapter(BaseAdapter):

    document_bundle_ids = dict()

    bundles = dict() #dict for alle bundles including record and relation information

    all_records = dict() # separate dict for records only (to get them by id)


    def __init__(self, *args):
        super(SimpleInMemoryAdapter, self).__init__()
        pass


    def connect(self, authentication_info):
        if authentication_info is not None:
            raise InvalidOptionsException()

        return True

    def create_document(self):
        #create new document id and return this is as stirng

        doc_id = str(uuid4())

        self.document_bundle_ids.update({doc_id: list()})

        #encode attributes and metadata
        attr = encode_dict_values_to_primitive(dict())
        meta = encode_dict_values_to_primitive(dict())


        doc_record = DbRecord(attr,meta)

        self.bundles.update({doc_id: DbBundle(list(),doc_record)})

        return doc_id

    def create_bundle(self, document_id, attributes, metadata):
        #save the bundle information and return id as string
        document_id = document_id
        bundle_id = str(uuid4())


        #transform the attributes and metadata to primitive data types
        attr = encode_dict_values_to_primitive(attributes)
        meta = encode_dict_values_to_primitive(metadata)



        self.bundles.update({bundle_id: DbBundle(list(), DbRecord(attr,meta))})

        doc = self.document_bundle_ids.get(document_id)
        doc.append(bundle_id)

        return bundle_id

    def create_relation(self, from_bundle_id, from_node, to_bundle_id, to_node, attributes, metadata):
        #save all relation information and return the relation id as string

        from_bundle = self.bundles.get(from_bundle_id)
        to_bundle = self.bundles.get(to_bundle_id)

        if from_bundle is None or to_bundle is None:
            raise NotFoundException()

        new_rel_id = str(uuid4())

        from_bundle.records.append(new_rel_id)

        # transform the attributes and metadata to primitive data types
        attr = encode_dict_values_to_primitive(attributes)
        meta = encode_dict_values_to_primitive(metadata)

        db_relations = DbRelation(attr,meta)

        self.all_records.update({new_rel_id:db_relations})

        return new_rel_id

    def create_record(self, bundle_id, attributes, metadata):
        #save all record information and return record id as string

        bundle_id = bundle_id
        bundle = self.bundles.get(bundle_id)


        new_rec_id = str(uuid4())

        bundle.records.append(new_rec_id)

        attr = encode_dict_values_to_primitive(attributes)
        meta = encode_dict_values_to_primitive(metadata)

        db_record = DbRecord(attr, meta)

        self.all_records.update({new_rec_id: db_record})

        return new_rec_id


    def get_document(self, document_id):
        #Should return a namedtuple of type DbDocument
        bundle_ids =  self.document_bundle_ids.get(document_id)
        doc_bundle = self.get_bundle(document_id)


        db_bundles = list()
        for bundle_id in bundle_ids:
            db_bundles.append(self.get_bundle(bundle_id))

        return DbDocument(doc_bundle,db_bundles)

    def get_record(self, record_id):
        db_record = self.all_records.get(record_id)
        if db_record is None:
            raise  NotFoundException()
        return db_record

    def get_relation(self, relation_id):
        return self.get_record(relation_id)

    def get_bundle(self, bundle_id):
        bundle = self.bundles.get(bundle_id)
        if bundle is None:
            raise NotFoundException()

        records = list()
        for record_id in bundle.records:
            record =self.all_records.get(record_id)
            records.append(record)

        return DbBundle(records, bundle.bundle_record)

    def delete_document(self, document_id):
        bundle_ids = self.document_bundle_ids.get(document_id)

        if bundle_ids is None:
            raise  NotFoundException()

        self.delete_bundle(document_id)
        for bundle_id in bundle_ids:
            self.delete_bundle(bundle_id)

        return True

    def delete_relation(self, relation_id):

        return self.delete_record(relation_id)

    def delete_bundle(self, bundle_id):
        bundle = self.bundles.get(bundle_id)
        if bundle is None:
            raise  NotFoundException()

        for record_id in bundle.records:
            record = self.all_records.get(record_id)

            if record is None:
                raise  NotFoundException()

            del self.all_records[record_id]
            del record

        del self.bundles[bundle_id]
        del bundle
        return True

    def delete_record(self, record_id):
        for bundle_id,bundle in self.bundles.items():
            relation_index = -1
            try:
                relation_index = bundle.records.index(record_id)
            except ValueError as e:
                pass

            if relation_index is not -1:
                del bundle.records[relation_index]

        record = self.all_records.get(record_id)

        if  record is None:
            raise NotFoundException()

        del self.all_records[record_id]

        return True






