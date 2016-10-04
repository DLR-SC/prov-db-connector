from functools import reduce
from io import BufferedReader

import six
from prov.model import ProvDocument

import logging
log = logging.getLogger(__name__)


class ConverterException(Exception):
    pass


class ParseException(ConverterException):
    pass


class NoDocumentException(ConverterException):
    pass


def form_string(content):
    """
    Take a string or BufferdReader as argument and transform the string into a ProvDocument
    :param content: Takes a sting or BufferedReader
    :return:ProvDocument
    """
    if isinstance(content, ProvDocument):
        return content
    elif isinstance(content, BufferedReader):
        content = reduce(lambda total, a: total + a, content.readlines())

    if type(content) is six.binary_type:
        content_str = content[0:15].decode()
        if content_str.find("{") > -1:
            return ProvDocument.deserialize(content=content, format='json')
        if content_str.find('<?xml') > -1:
            return ProvDocument.deserialize(content=content, format='xml')
        elif content_str.find('document') > -1:
            return ProvDocument.deserialize(content=content, format='provn')

    raise ParseException("Unsupported input type {}".format(type(content)))


def to_json(document=None):
    if document is None:
        raise NoDocumentException()
    return document.serialize(format='json')


def from_json(document=None):
    if document is None:
        raise NoDocumentException()
    return ProvDocument.deserialize(source=document, format='json')


def to_provn(document=None):
    if document is None:
        raise NoDocumentException()
    return document.serialize(format='provn')


def from_provn(document=None):
    if document is None:
        raise NoDocumentException()
    return ProvDocument.deserialize(source=document, format='provn')


def to_xml(document=None):
    if document is None:
        raise NoDocumentException()
    return document.serialize(format='xml')


def from_xml(document=None):
    if document is None:
        raise NoDocumentException()
    return ProvDocument.deserialize(source=document, format='xml')
