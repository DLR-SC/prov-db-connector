import json
import logging
import sys
from datetime import datetime
from io import StringIO

import six
from prov.constants import PROV_QUALIFIEDNAME, PROV_ATTRIBUTES_ID_MAP, PROV_ATTRIBUTES, PROV_MEMBERSHIP, \
    PROV_ATTR_ENTITY, PROV_ATTRIBUTE_QNAMES, PROV_ATTR_COLLECTION, XSD_ANYURI
from prov.model import Literal, Identifier, QualifiedName, Namespace, parse_xsd_datetime

from provdbconnector.db_adapters.baseadapter import METADATA_KEY_NAMESPACES


class SerializerException(Exception):
    pass


logger = logging.getLogger(__name__)
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


def encode_dict_values_to_primitive(dict_values):
    dict_values = dict_values.copy()
    for key, value in dict_values.items():
        dict_values[key] = encode_string_value_to_primitive(value)

    return dict_values


def encode_string_value_to_primitive(value):
    if sys.version_info[0] < 3:
        if type(value) is unicode:
            return value.encode("utf8")
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
    # TODO: QName export
    value, datatype, langtag = literal.value, literal.datatype, literal.langtag
    if langtag:
        return {'lang': langtag}
    else:
        return {'type': six.text_type(datatype)}


def encode_json_representation(value):
    if isinstance(value, Literal):
        return literal_json_representation(value)
    elif isinstance(value, datetime):
        return {'type': 'xsd:dateTime'}
    elif isinstance(value, QualifiedName):
        # TODO Manage prefix in the whole structure consistently
        # TODO QName export
        return {'type': PROV_QUALIFIEDNAME._str}
    elif isinstance(value, Identifier):
        return {'type': 'xsd:anyURI'}
    elif type(value) in LITERAL_XSDTYPE_MAP:
        return {'type': LITERAL_XSDTYPE_MAP[type(value)]}
    else:
        return None


# DECODE

def add_namespaces_to_bundle(prov_bundle, metadata):
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
    else:
        raise SerializerException(
            "Namespaces metadata should returned as json string or dict not as {}".format(type(namespace_str)))

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

    :param prov_type: valid prov type like prov:Entry as string
    :param prov_id: valid id as string like <namespace>:<name>
    :param properties: dict{attr_name:attr_value} dict with all properties (prov and additional)
    :return: ProvRecord
    """
    # Parse attributes
    if isinstance(properties, dict):
        properties_list = properties.items()
    elif isinstance(properties, list):
        properties_list = properties
    else:
        raise SerializerException(
            "please provide properties as list[(key,value)] or dict your provided: %s" % properties.__class__.__name__)

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
            value = (
                bundle.valid_qualified_name(value)
                if attr in PROV_ATTRIBUTE_QNAMES
                else parse_xsd_datetime(value)
            )
            attributes[attr] = value
        else:
            value_type = None
            if type_map:
                value_type = type_map.get(attr_name, None)

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
