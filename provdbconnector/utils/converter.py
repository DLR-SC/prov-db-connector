from prov.model import ProvDocument
from prov.serializers.provjson import ProvJSONSerializer

class ConverterException(Exception):
    pass


class ParseException(ConverterException):
    pass

class NoDocumentException(ConverterException):
    pass


def to_json(document=None):
    if document is None:
        raise NoDocumentException()
    return document.serialize(format='json')


def from_json(document=None):
    if document is None:
        raise NoDocumentException()
    return ProvDocument.deserialize(document,format='json')


def to_provn(document=None):
    if document is None:
        raise NoDocumentException()
    return document.serialize(format='provn')


def from_provn(document=None):
    if document is None:
        raise NoDocumentException()
    return ProvDocument.deserialize(document,format='provn')

def to_xml(document=None):
    if document is None:
        raise NoDocumentException()
    return document.serialize(format='xml')


def from_xml(document=None):
    if document is None:
        raise NoDocumentException()
    return ProvDocument.deserialize(document,format='xml')
