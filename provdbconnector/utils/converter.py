from functools import reduce
from io import BufferedReader
from provdbconnector.exceptions.utils import ParseException, NoDocumentException

import six
from prov.model import ProvDocument

import logging
log = logging.getLogger(__name__)


def form_string(content):
    """
    Take a string or BufferedReader as argument and transform the string into a ProvDocument

    :param content: Takes a sting or BufferedReader
    :return: ProvDocument
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
    """
    Try to convert a ProvDocument into the json representation

    :param document:
    :type document: prov.model.ProvDocument
    :return: Json string of the document
    :rtype: str
    """
    if document is None:
        raise NoDocumentException()
    return document.serialize(format='json')


def from_json(json=None):
    """
    Try to convert a json string into a document

    :param json: The json str
    :type json: str
    :return: Prov Document
    :rtype: prov.model.ProvDocument
    :raise: NoDocumentException
    """
    if json is None:
        raise NoDocumentException()
    return ProvDocument.deserialize(source=json, format='json')


def to_provn(document=None):
    """
    Try to convert a document into a provn representation

    :param document: Prov document to convert
    :type document: prov.model.ProvDocument
    :return: The prov-n str
    :rtype: str
    :raise: NoDocumentException
    """
    if document is None:
        raise NoDocumentException()
    return document.serialize(format='provn')


def from_provn(provn_str=None):
    """
    Try to convert a provn string into a ProvDocument

    :param provn_str: The string to convert
    :type provn_str: str
    :return: The Prov document
    :rtype: ProvDocument
    :raises: NoDocumentException
    """
    if provn_str is None:
        raise NoDocumentException()
    return ProvDocument.deserialize(source=provn_str, format='provn')


def to_xml(document=None):
    """
    Try to convert a document into an xml string

    :param document: The ProvDocument to convert
    :param document: ProvDocument
    :return: The xml string
    :rtype: str
    """
    if document is None:
        raise NoDocumentException()
    return document.serialize(format='xml')


def from_xml(xml_str=None):
    """
    Try to convert a xml string into a ProvDocument

    :param xml_str: The xml string
    :type xml_str: str
    :return: The Prov document
    :rtype: ProvDocument
    """
    if xml_str is None:
        raise NoDocumentException()
    return ProvDocument.deserialize(source=xml_str, format='xml')
