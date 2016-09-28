from uuid import uuid4
from prov.model import  ProvDocument

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

    def get_document_as_prov(self, id=None):
        raise NotImplementedError()
