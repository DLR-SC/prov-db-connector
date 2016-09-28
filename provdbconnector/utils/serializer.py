from prov.model import Literal,Identifier, QualifiedName
from prov.constants import PROV_QUALIFIEDNAME
from datetime import datetime
import six
import sys


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
    for key,value in dict_values.items():
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

