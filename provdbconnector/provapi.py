from uuid import uuid4
from prov.model import  ProvDocument, ProvBundle, ProvRecord,ProvElement,ProvRelation, QualifiedName, ProvMention
from prov.constants import PROV_ATTRIBUTES, PROV_N_MAP, PROV_MENTION
from provdbconnector.databases.baseadapter import METADATA_KEY_PROV_TYPE,METADATA_PARENT_ID,METADATA_KEY_LABEL,METADATA_KEY_NAMESPACES,METADATA_KEY_BUNDLE_ID,METADATA_KEY_TYPE_MAP
from provdbconnector.utils.serializer import encode_json_representation
from collections import namedtuple
class ProvApiException(Exception):
    pass


class NoDataBaseAdapterException(ProvApiException):
    pass

class InvalidArgumentTypeException(ProvApiException):
    pass
class InvalidProvRecordException(ProvApiException):
    pass
class ProvApi(object):
    def __init__(self, id=None, adapter=None, authinfo=None, *args):
        if id is None:
            self.apiid = uuid4()
        else:
            self.apiid = id

        if adapter is None:
            raise NoDataBaseAdapterException()
        self._adapter = adapter()
        self._adapter.connect(authinfo)

    #Converter Methods
    def create_document_from_json(self, content=None):
        raise NotImplementedError()

    def get_document_as_json(self, id=None):
        raise NotImplementedError()

    def create_document_from_xml(self, content=None):
        raise NotImplementedError()

    def get_document_as_xml(self, id=None):
        raise NotImplementedError()

    def create_document_from_provn(self, content=None):
        raise NotImplementedError()

    def get_document_as_provn(self, id=None):
        raise NotImplementedError()

    #Methods that consume ProvDocument instances and produce ProvDocument instances
    def create_document_from_prov(self, content=None):
        if not isinstance(content,ProvDocument):
            raise InvalidArgumentTypeException()

        prov_document = content

        doc_id = self._adapter.create_document()

        self._create_bundle(doc_id,prov_document,bundle_connection=False)

        # foreach bundle
        #    create bundle id
        #    self._create_bundle(bundle_id, bundle)


        # foreach bundle in bundles
        #    self._create_bundle_links(bundle_id, prov_bundle)


    def get_document_as_prov(self, id=None):
        raise NotImplementedError()


    def _create_bundle(self,bundle_id,prov_bundle, bundle_connection=True):
        if not isinstance(prov_bundle, ProvBundle) or type(bundle_id) is not str:
            raise InvalidArgumentTypeException()


        for record in prov_bundle.get_records(ProvElement):
            (metadata,attributes) = self._get_metadata_and_attributes_for_record(record)
            self._adapter.create_record(bundle_id,attributes,metadata)
        #        create relation between bundle node and record node, only if options (bundle_connection) is set


        for relation in prov_bundle.get_records(ProvRelation):
            #skip relations of the type "prov:mentionOf" https://www.w3.org/TR/prov-links/
            if relation.get_type() is PROV_MENTION:
                continue

            from_tuple, to_tuple= relation.formal_attributes[:2]
            from_qualified_name = from_tuple[1]
            to_qualified_name = to_tuple[1]
            #if target or origin record is unknown, create node "Unknown"

            (metadata,attributes) = self._get_metadata_and_attributes_for_record(relation)
            self._adapter.create_relation(bundle_id,from_qualified_name,bundle_id,to_qualified_name, attributes,metadata)





    def _create_bundle_links(self,prov_bundle):

        #   foreach relation in bundle
        #        if the relation type is"prov:mentionOf" (https://www.w3.org/TR/prov-links/)
        #             create relation
        pass

    def _get_metadata_and_attributes_for_record(self, prov_record):
        if not isinstance(prov_record, ProvRecord):
            raise InvalidArgumentTypeException()

        used_namespaces = dict()
        bundle = prov_record.bundle

        prov_type = prov_record.get_type()
        prov_label = prov_record.label

        #if relation without label -> use prov_type as label
        if prov_label is None and prov_record.identifier is None:
            prov_label = prov_type

        # Be sure that the prov_label is a qualified name instnace

        if not isinstance(prov_label, QualifiedName):
            qualified_name = bundle.valid_qualified_name(prov_label)
            if qualified_name is None:
                raise InvalidProvRecordException("The prov record {} is invalid because the prov_label {} can't be qualified".format(prov_record,prov_label))
            else:
                prov_label =  qualified_name

        #extract namespaces from record

        #add namespace from prov_type
        namespace = prov_type.namespace
        used_namespaces.update({str(namespace.prefix): str(namespace.uri)})

        #add namespace from prov label
        namespace = prov_label.namespace
        used_namespaces.update({str(namespace.prefix): str(namespace.uri)})

        attributes = dict(prov_record.attributes.copy())
        for key,value in attributes.items():

            #ensure key is QualifiedName
            if isinstance(key, QualifiedName):
                namespace = key.namespace
                used_namespaces.update({str(namespace.prefix): str(namespace.uri)})
            else:
                raise InvalidProvRecordException("Not support key type %s" % type(key))

            #try to add
            if isinstance(value, QualifiedName):
                namespace = value.namespace
                used_namespaces.update({str(namespace.prefix): str(namespace.uri)})
            else:
                qualified_name = bundle.valid_qualified_name(value)
                if qualified_name is not None:
                    attributes[key] = qualified_name # update attribute
                    namespace = qualified_name.namespace
                    used_namespaces.update({str(namespace.prefix): str(namespace.uri)})

        #create type dict
        types_dict = dict()
        for key,value in attributes.items():
            if key not in PROV_ATTRIBUTES:
                type = encode_json_representation(value)
                if type is not None:
                    types_dict.update({str(key): type})

        metadata = {
            METADATA_KEY_PROV_TYPE: prov_type,
            METADATA_KEY_LABEL: prov_label,
            METADATA_KEY_NAMESPACES: used_namespaces,
            METADATA_KEY_TYPE_MAP: types_dict
        }
        MetaAndAttributes = namedtuple("MetaAndAttributes", "metadata, attributes")

        return MetaAndAttributes(metadata,attributes)

