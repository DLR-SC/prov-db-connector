import json
import logging
import sys
from collections import namedtuple
from datetime import datetime
from io import StringIO

import six
from prov.constants import PROV_QUALIFIEDNAME, PROV_ATTRIBUTES_ID_MAP, PROV_ATTRIBUTES, PROV_MEMBERSHIP, \
    PROV_ATTR_ENTITY, PROV_ATTRIBUTE_QNAMES, PROV_ATTR_COLLECTION, XSD_ANYURI, PROV_ATTR_ACTIVITY, PROV_ATTR_AGENT, \
    PROV_ATTR_TRIGGER,PROV_ATTR_INFORMED,PROV_ATTR_INFORMANT,PROV_ATTR_STARTER,PROV_ATTR_ENDER,PROV_ATTR_AGENT \
    ,PROV_ATTR_PLAN,PROV_ATTR_DELEGATE,PROV_ATTR_RESPONSIBLE,PROV_ATTR_GENERATED_ENTITY,PROV_ATTR_USED_ENTITY, \
    PROV_ATTR_GENERATION,PROV_ATTR_USAGE,PROV_ATTR_SPECIFIC_ENTITY,PROV_ATTR_GENERAL_ENTITY,PROV_ATTR_ALTERNATE1, \
    PROV_ATTR_ALTERNATE2,PROV_ATTR_BUNDLE,PROV_ATTR_INFLUENCEE,PROV_ATTR_INFLUENCER

from prov.model import Literal, Identifier, QualifiedName, Namespace, parse_xsd_datetime, PROV_REC_CLS, ProvAgent, \
    ProvEntity, ProvActivity, ProvElement
from provdbconnector.db_adapters.baseadapter import METADATA_KEY_NAMESPACES, METADATA_KEY_PROV_TYPE, \
    METADATA_KEY_TYPE_MAP
from provdbconnector.exceptions.database import MergeException
from provdbconnector.exceptions.provapi import InvalidArgumentTypeException
from provdbconnector.exceptions.utils import SerializerException

log = logging.getLogger(__name__)

logger = logging.getLogger(__name__)

FormalAndOtherAttributes = namedtuple("formal_and_other_attributes", "formal, other")

# Reverse map for prov.model.XSD_DATATYPE_PARSERS
LITERAL_XSDTYPE_MAP = {
    float: 'xsd:double',
    int: 'xsd:int'
    # boolean, string values are supported natively by PROV-JSON
    # datetime values are converted separately
}

# Add long on Python 2
if six.integer_types[-1] not in LITERAL_XSDTYPE_MAP:
    LITERAL_XSDTYPE_MAP[six.integer_types[-1]] = 'xsd:long'


PROV_ATTR_BASE_CLS = {
    PROV_ATTR_ENTITY: ProvEntity,
    PROV_ATTR_ACTIVITY: ProvActivity,
    PROV_ATTR_TRIGGER: ProvEntity,
    PROV_ATTR_INFORMED: ProvActivity,
    PROV_ATTR_INFORMANT: ProvActivity,
    PROV_ATTR_STARTER: ProvActivity,
    PROV_ATTR_ENDER: ProvActivity,
    PROV_ATTR_AGENT: ProvAgent,
    PROV_ATTR_PLAN: ProvEntity,
    PROV_ATTR_DELEGATE: ProvAgent,
    PROV_ATTR_RESPONSIBLE: ProvAgent,
    PROV_ATTR_GENERATED_ENTITY: ProvEntity,
    PROV_ATTR_USED_ENTITY: ProvEntity,
    PROV_ATTR_GENERATION: ProvElement,
    PROV_ATTR_USAGE: ProvElement,
    PROV_ATTR_SPECIFIC_ENTITY: ProvEntity,
    PROV_ATTR_GENERAL_ENTITY: ProvEntity,
    PROV_ATTR_ALTERNATE1: ProvEntity,
    PROV_ATTR_ALTERNATE2: ProvEntity,
    PROV_ATTR_BUNDLE:ProvElement,
    PROV_ATTR_INFLUENCEE: ProvElement,
    PROV_ATTR_INFLUENCER: ProvElement,
    PROV_ATTR_COLLECTION: ProvEntity
}

def encode_dict_values_to_primitive(dict_values):
    """
    This function transforms a dict with all kind of types into a dict with only

    - str
    - dict
    - book
    - str

    values

    :param dict_values:
    :return:
    """
    new_dict_values = dict()
    for key, value in dict_values.items():
        key_simple = str(key)
        new_dict_values.update({key_simple: encode_string_value_to_primitive(value)})

    return new_dict_values


def encode_string_value_to_primitive(value):
    """
    Convert a value into one of the following types:

    - dict
    - str
    - float
    - int
    - list


    :param value:
    :return:
    """
    if isinstance(value, Literal):
        return value.value
    elif type(value) is int:
        return value
    elif type(value) is float:
        return value
    elif type(value) is bool:
        return value
    elif type(value) is list:
        return value
    elif type(value) is dict:
        io = StringIO()
        json.dump(value, io)
        return io.getvalue()
    return str(value)


def literal_json_representation(literal):
    """
    Some internationalization stuff

    :param literal:
    :return:
    """
    # TODO: QName export
    value, datatype, langtag = literal.value, literal.datatype, literal.langtag
    if langtag:
        return {'lang': langtag}
    else:
        return {'type': six.text_type(datatype)}


def encode_json_representation(value):
    """
    Get the type of a value

    :param value:
    :return:
    """
    if isinstance(value, Literal):
        return literal_json_representation(value)
    elif isinstance(value, datetime):
        return {'type': 'xsd:dateTime'}
    elif isinstance(value, QualifiedName):
        # TODO Manage prefix in the whole structure consistently
        # TODO QName export
        return {'type': str(PROV_QUALIFIEDNAME)}
    elif isinstance(value, Identifier):
        return {'type': 'xsd:anyURI'}
    elif type(value) in LITERAL_XSDTYPE_MAP:
        return {'type': LITERAL_XSDTYPE_MAP[type(value)]}
    else:
        return None


# DECODE

def add_namespaces_to_bundle(prov_bundle, metadata):
    """
    Add all namespaces in the metadata_dict to the provided bundle

    :param prov_bundle:
    :param metadata:
    :return: None
    """
    namespaces = dict()
    try:
        namespace_str = metadata[METADATA_KEY_NAMESPACES]
    except ValueError:
        SerializerException("No valid namespace provided, should be a string of a dict: {}".format(metadata))
        return

    if type(namespace_str) is str:
        io = StringIO(namespace_str)
        namespaces = json.load(io)
    elif type(namespace_str) is dict:
        namespaces = namespace_str
    elif type(namespace_str) is list:
        for entry in namespace_str:
            if type(entry) is str:
                io = StringIO(entry)
                namespaces.update(json.load(io))
            else:
                raise SerializerException(
                    "Namespaces metadata should returned as json string dict or list of json strings not as {}".format(
                        type(namespace_str)))

    else:
        raise SerializerException(
            "Namespaces metadata should returned as json string dict or list of json strings not as  {}".format(
                type(namespace_str)))

    for prefix, uri in namespaces.items():
        if prefix is not None and uri is not None:
            if prefix != 'default':
                prov_bundle.add_namespace(Namespace(prefix, uri))
            else:
                prov_bundle.set_default_namespace(uri)
        else:
            SerializerException("No valid namespace provided for the metadata: {}".format(metadata))


def create_prov_record(bundle, prov_type, prov_id, properties, type_map):
    """

    :param bundle:
    :param prov_type: valid prov type like prov:Entry as string
    :param prov_id: valid id as string like <namespace>:<name>
    :param properties: dict{attr_name:attr_value} dict with all properties (prov and additional)
    :param type_map: dict{attr_name:type_str} Contains the type information for each property (only if type is necessary)
    :return: ProvRecord
    """
    # Parse attributes
    if isinstance(properties, dict):
        properties_list = properties.items()
    elif isinstance(properties, list):
        properties_list = properties
    else:
        raise SerializerException(
            "Please provide properties as list[(key,value)] or dict your provided: {}".format(
                properties.__class__.__name__))

    attributes = dict()
    other_attributes = []
    # this is for the multiple-entity membership hack to come
    membership_extra_members = None
    for attr_name, values in properties_list:

        attr = (
            PROV_ATTRIBUTES_ID_MAP[attr_name]
            if attr_name in PROV_ATTRIBUTES_ID_MAP
            else bundle.valid_qualified_name(attr_name)
        )
        if attr in PROV_ATTRIBUTES:
            if isinstance(values, list):
                # only one value is allowed
                if len(values) > 1:
                    # unless it is the membership hack
                    if prov_type == PROV_MEMBERSHIP and \
                                    attr == PROV_ATTR_ENTITY:
                        # This is a membership relation with
                        # multiple entities
                        # HACK: create multiple membership
                        # relations, one x each entity

                        # Store all the extra entities
                        membership_extra_members = values[1:]
                        # Create the first membership relation as
                        # normal for the first entity
                        value = values[0]
                    else:
                        error_msg = (
                            'The prov package does not support PROV'
                            ' attributes having multiple values.'
                        )
                        logger.error(error_msg)
                        raise SerializerException(error_msg)
                else:
                    value = values[0]
            else:
                value = values

            if attr in PROV_ATTRIBUTE_QNAMES:
                value = (bundle.valid_qualified_name(value))
            elif isinstance(value, datetime):
                value = value
            else:
                parse_xsd_datetime(value)

            attributes[attr] = value
        else:
            value_type = None
            if type_map:
                value_type = type_map.get(attr_name)

            if isinstance(values, list):
                other_attributes.extend(
                    (
                        attr,
                        decode_json_representation(value, value_type, bundle)
                    )
                    for value in values
                )
            else:
                # single value
                other_attributes.append(
                    (
                        attr,
                        decode_json_representation(values, value_type, bundle)
                    )
                )
    record = bundle.new_record(
        prov_type, prov_id, attributes, other_attributes
    )
    # HACK: creating extra (unidentified) membership relations
    if membership_extra_members:
        collection = attributes[PROV_ATTR_COLLECTION]
        for member in membership_extra_members:
            bundle.membership(
                collection, bundle.valid_qualified_name(member)
            )
    return record


def decode_json_representation(value, type, bundle):
    """
    Return the value based on the type see also encode_json_representation

    :param value:
    :param type:
    :param bundle:
    :return:
    """
    if isinstance(type, dict):
        # complex type
        datatype = type['type'] if 'type' in type else None
        datatype = bundle.valid_qualified_name(datatype)
        langtag = type['lang'] if 'lang' in type else None
        if datatype == XSD_ANYURI:
            return Identifier(value)
        elif datatype == PROV_QUALIFIEDNAME:
            return bundle.valid_qualified_name(value)
        else:
            # The literal of standard Python types is not converted here
            # It will be automatically converted when added to a record by
            # _auto_literal_conversion()
            return Literal(value, datatype, langtag)
    else:
        # simple type, just return it
        return value


def split_into_formal_and_other_attributes(attributes, metadata):
    """
    This function split the attributes and metadata into formal attributes and other attributes.
    Helpful for merge operations and searching for duplicate relations

    :param attributes:
    :param metadata:
    :return: namedtuple(formal_attributes, other_attributes)
    :rtype: FormalAndOtherAttributes
    """
    prov_type = metadata[METADATA_KEY_PROV_TYPE]

    if str(prov_type) == "prov:Unknown":
        formal_qualified_names = set()
    else:
        class_type = PROV_REC_CLS[prov_type]
        formal_qualified_names = class_type.FORMAL_ATTRIBUTES

    formal_attributes = {key: attributes[key] for key in attributes.keys() if key in formal_qualified_names}
    other_attributes = {key: attributes[key] for key in attributes.keys() if key not in formal_qualified_names}

    return FormalAndOtherAttributes(formal_attributes, other_attributes)

def merge_record(attributes, metadata, other_attributes, other_metadata):
    """
    Merge 2 records into one

    :param attributes: The original attributes
    :param metadata: The original metadata
    :param other_attributes: The attributes to merge
    :param other_metadata:  The metadata to merge
    :return: tuple(attributes, metadata)
    :rtype: Tuple(attributes,metadata)
    """
    attributes_merged = attributes.copy()
    attributes_merged.update(other_attributes)

    metadata_prov_typ = metadata[METADATA_KEY_PROV_TYPE]
    other_metadata_prov_typ = other_metadata[METADATA_KEY_PROV_TYPE]

    # Determinate non unknown prov type for merge
    merged_prov_typ = other_metadata_prov_typ
    is_one_prov_type_unknown = False
    # Support unknown typ during the merge process, needed for polymorph nodes
    if other_metadata_prov_typ.localpart == "Unknown":
        merged_prov_typ = metadata_prov_typ
        is_one_prov_type_unknown = True

    if metadata_prov_typ.localpart == "Unknown":
        merged_prov_typ = other_metadata_prov_typ
        is_one_prov_type_unknown = True

    if merged_prov_typ.localpart == "Unknown":
        raise MergeException("Prov type can't be unknown metadata: {}, other metadata: {}".format(metadata_prov_typ, other_metadata_prov_typ))

    if metadata_prov_typ != other_metadata_prov_typ and not is_one_prov_type_unknown:
        raise MergeException(
            "Prov type should be the same but is: {}:{}".format(metadata_prov_typ, other_metadata_prov_typ))

    for (key, value) in attributes.items():
        if attributes_merged[key] != value:
            raise MergeException(
                "Invalid data, it is not allowed to override existing attributes key:{}, value:{} with value:{}".format(
                    key, value, attributes_merged[key]))

    merged_metadata_namespaces = metadata[METADATA_KEY_NAMESPACES].copy()
    merged_metadata_namespaces.update(other_metadata[METADATA_KEY_NAMESPACES])

    merged_metadata_type_map = metadata[METADATA_KEY_TYPE_MAP].copy()
    merged_metadata_type_map.update(other_metadata[METADATA_KEY_TYPE_MAP])

    merged_metadata = metadata.copy()
    merged_metadata.update(other_metadata)
    merged_metadata.update({METADATA_KEY_PROV_TYPE: merged_prov_typ})
    merged_metadata.update({METADATA_KEY_NAMESPACES: merged_metadata_namespaces})
    merged_metadata.update({METADATA_KEY_TYPE_MAP: merged_metadata_type_map})

    return attributes_merged, merged_metadata

def serialize_namespace(namespace: Namespace):
    prefix = namespace.prefix

    if prefix == "" or prefix is None:
        prefix = "default"
    return {str(prefix): str(namespace.uri)}
