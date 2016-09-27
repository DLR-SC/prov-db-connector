import sys

from prov.model import Literal


def encode_string_value_to_premitive(value):
    if sys.version_info[0] < 3:
        if type(value) is unicode:
            return value.encode("utf8")
    if isinstance(value, Literal):
        return value.value
    elif type(value) is bool:
        return value
    return str(value)
