from prov.model import Literal
import sys
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


