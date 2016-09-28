from uuid import uuid4
from prov.model import  ProvDocument, ProvBundle

class ProvApiException(Exception):
    pass


class NoDataBaseAdapterException(ProvApiException):
    pass

class InvalidArgumentTypeException(ProvApiException):
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

        # create document id
        # create_bundle(document_id, prov_document)

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

        #    foreach record in bundle :
        #        prepare metadata (like labels, namespaces, type_map) as primitive datatypes
        #        create database node
        #        create relation between bundle node and record node, only if options (bundle_connection) is set


        #    foreach relation in bundle:
        #        skip relations of the type "prov:mentionOf"
        #        if target or origin record is unknown, create node "Unknown"
        #        prepare metadata (like labels, namespaces, type_map) as primitive datatypes
        #        create database relation

    def _create_bundle_links(self,prov_bundle):

        #   foreach relation in bundle
        #        if the relation type is"prov:mentionOf" (https://www.w3.org/TR/prov-links/)
        #             create relation
        pass
