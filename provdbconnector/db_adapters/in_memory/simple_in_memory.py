from provdbconnector.db_adapters.baseadapter import  BaseAdapter, DbDocument,DbBundle,DbRecord,DbRelation, InvalidOptionsException, NotFoundException, METADATA_KEY_IDENTIFIER
from collections import  namedtuple
from provdbconnector.utils.serializer import encode_dict_values_to_primitive
from uuid import uuid4
import logging
log = logging.getLogger(__name__)


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

        doc_record = self._create_db_record(dict(), dict())

        self.bundles.update({doc_id: DbBundle(list(),doc_record)})

        return doc_id

    def create_bundle(self, document_id, attributes, metadata):
        #save the bundle information and return id as string
        document_id = document_id
        bundle_id = str(uuid4())

        self.bundles.update({bundle_id: DbBundle(list(), self._create_db_record(attributes, metadata))})

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

        db_relations = DbRelation(metadata,attributes)

        self.all_records.update({new_rel_id:db_relations})

        return new_rel_id

    def create_record(self, bundle_id, attributes, metadata):
        #save all record information and return record id as string

        bundle_id = bundle_id
        bundle = self.bundles.get(bundle_id)


        new_rec_id = str(uuid4())

        bundle.records.append(new_rec_id)

        db_record = self._create_db_record(attributes, metadata)

        self.all_records.update({new_rec_id: db_record})

        return new_rec_id


    def get_document(self, document_id):
        #Should return a namedtuple of type DbDocument
        doc =  self.documents.get(document_id)
        db_records = doc.records


        db_bundles = list()
        for bundle_id in doc.bundles:
            db_bundles.append(self.get_bundle(bundle_id))

        return DbDocument(db_records,db_bundles)

    def get_record(self, record_id):
        super().get_record(record_id)

    def get_relation(self, relation_id):
        super().get_relation(relation_id)

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
        super().delete_document(document_id)

    def delete_relation(self, relation_id):
        super().delete_relation(relation_id)

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
        super().delete_record(record_id)

    def _create_db_record(self, attributes, metadata):

        attr = encode_dict_values_to_primitive(attributes)
        meta = encode_dict_values_to_primitive(metadata)

        return DbRecord(attr,meta)


