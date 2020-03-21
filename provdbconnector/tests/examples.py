from prov.tests.examples import primer_example as primer_example, \
    primer_example_alternate, \
    w3c_publication_1, \
    w3c_publication_2, \
    bundles1, \
    bundles2, \
    collections, \
    long_literals, \
    datatypes

from datetime import datetime
from collections import namedtuple

from prov.constants import PROV_RECORD_IDS_MAP, PROV
from prov.model import ProvDocument, ProvActivity, Literal, Identifier
from provdbconnector.db_adapters.baseadapter import METADATA_KEY_NAMESPACES, METADATA_KEY_PROV_TYPE, \
    METADATA_KEY_TYPE_MAP, METADATA_KEY_IDENTIFIER


def prov_db_unknown_prov_typ_example():
    doc = ProvDocument()
    doc.add_namespace("ex", "https://example.com")
    doc.entity(identifier="ex:Entity1")
    doc.entity(identifier="ex:Entity2")
    doc.influence(influencee="ex:Entity1", influencer="ex:Entity2")
    return doc

def prov_default_namespace_example(ns_postfix: str):
    doc = ProvDocument()
    doc.set_default_namespace("https://example.com/{0}".format(ns_postfix))
    doc.entity(identifier="Entity1")
    return doc



def attributes_dict_example():
    """
    Retuns a example dict with some different attributes

    :return: dict with attributes
    :rtype: dict
    """
    attributes = dict()
    attributes.update({"ex:individual attribute": "Some value"})
    attributes.update({"ex:int value": 99})
    attributes.update({"ex:double value": 99.33})
    attributes.update({"ex:date value": datetime.strptime('Jun 1 2005  1:33PM', '%b %d %Y %I:%M%p')})
    attributes.update({"ex:list value": ["list", "of", "strings"]})
    attributes.update({"ex:dict value": {"dict": "value"}})

    return attributes


def base_connector_bundle_parameter_example():
    """
    This example returns a dict with example arguments for a db_adapter

    :return: dict {attributes, metadata}
    :rtype: dict
    """
    doc = ProvDocument()
    doc.add_namespace("ex", "http://example.com")
    attributes = dict()
    attributes.update({"prov:type": "prov:Bundle"})

    namespaces = dict()
    namespaces.update({"ex": "http://example.com"})

    type_map = dict()
    type_map.update({"int value": "int"})
    type_map.update({"date value": "xds:datetime"})

    metadata = dict()

    metadata.update({METADATA_KEY_PROV_TYPE: doc.valid_qualified_name("prov:Entity")})
    metadata.update({METADATA_KEY_IDENTIFIER: doc.valid_qualified_name("ex:bundle name")})
    metadata.update({METADATA_KEY_TYPE_MAP: type_map})
    metadata.update({METADATA_KEY_NAMESPACES: namespaces})

    return_data = dict()
    return_data.update({"attributes": attributes})
    return_data.update({"metadata": metadata})
    return return_data


def base_connector_record_parameter_example():
    """
    Returns a dict with attributes and metadata for a simple node

    :return:dict with attributes metadata
    :rtype: dict
    """
    doc = ProvDocument()

    namespaces = dict()
    namespaces.update({"ex": "http://example.com"})
    namespaces.update({"custom": "http://custom.com"})

    type_map = dict()
    type_map.update({"int value": "int"})
    type_map.update({"date value": "xds:datetime"})

    metadata = dict()

    metadata.update({METADATA_KEY_PROV_TYPE: doc.valid_qualified_name("prov:Activity")})
    metadata.update({METADATA_KEY_IDENTIFIER: doc.valid_qualified_name("prov:example_node")})
    metadata.update({METADATA_KEY_TYPE_MAP: type_map})
    metadata.update({METADATA_KEY_NAMESPACES: namespaces})

    return_data = dict()
    return_data.update({"attributes": attributes_dict_example()})
    return_data.update({"metadata": metadata})

    return return_data


def base_connector_relation_parameter_example():
    """
    Returns a example with a start nodes (attributes, metadata) and also a relation dict with attributes metadata

    :return: dict
    :rtype: dict
    """
    doc = ProvDocument()
    doc.add_namespace("ex", "http://example.com")
    doc.add_namespace("custom", "http://custom.com")

    namespaces = dict()
    namespaces.update({"ex": "http://example.com"})
    namespaces.update({"custom": "http://custom.com"})

    type_map = dict()
    type_map.update({"int value": "int"})
    type_map.update({"date value": "xds:datetime"})

    metadata = dict()

    metadata.update({METADATA_KEY_PROV_TYPE: PROV_RECORD_IDS_MAP["mentionOf"]})
    metadata.update({METADATA_KEY_IDENTIFIER: "identifier for the relation"})
    metadata.update({METADATA_KEY_TYPE_MAP: type_map})
    metadata.update({METADATA_KEY_NAMESPACES: namespaces})

    return_data = dict()
    return_data.update({"attributes": attributes_dict_example()})
    return_data.update({"metadata": metadata})
    return_data.update({"from_node": doc.valid_qualified_name("ex:Yoda")})
    return_data.update({"to_node": doc.valid_qualified_name("ex:Luke Skywalker")})
    return_data.update({"doc": doc})

    return return_data


def base_connector_merge_example():
    """
    This example returns a namedtuple with a from_node relation and to_node
    to test the merge behavior

    :return: namedtuple(from_node, relation, to_node)
    :rtype: namedtuple
    """
    # noinspection PyPep8Naming
    ReturnData = namedtuple("base_connector_merge_example_return_data", "from_node,relation,to_node")
    example_relation = base_connector_relation_parameter_example()

    example_node_a = base_connector_record_parameter_example()
    example_node_b = base_connector_record_parameter_example()

    example_node_a["metadata"][METADATA_KEY_IDENTIFIER] = example_relation["from_node"]
    example_node_b["metadata"][METADATA_KEY_IDENTIFIER] = example_relation["to_node"]

    return ReturnData(example_node_a, example_relation, example_node_b)


def prov_api_record_example():
    """
    This is a more complex record example

    :return:
    """
    doc = ProvDocument()
    doc.add_namespace("ex", "http://example.com")
    doc.add_namespace("custom", "http://custom.com")

    attributes = attributes_dict_example()
    del attributes[
        "ex:dict value"]  # remove dict value because it is not allowed in a prov_record, but for low level adapter tests necessary
    del attributes[
        "ex:list value"]  # remove dict value because it is not allowed in a prov_record, but for low level adapter tests necessary
    attributes.update({"ex:Qualified name ": doc.valid_qualified_name("custom:qualified name")})
    attributes.update({"ex:Qualified name 2": "ex:unqualified_name"})
    attributes.update({"ex:Literal": Literal("test literal", langtag="en")})
    attributes.update({"ex:Literal 2": Literal("test literal with datatype", langtag="en",
                                               datatype=PROV["InternationalizedString"])})
    attributes.update({"ex:identifier type": Identifier("http://example.com/#test")})

    expected_attributes = dict()
    for key, value in attributes.items():
        new_key = doc.valid_qualified_name(key)
        expected_attributes.update({new_key: value})

    # The prov lib don't require to auto convert string values into qualified names
    # valid_name = doc.valid_qualified_name("ex:Qualified name 2")
    # expected_attributes[valid_name] = doc.valid_qualified_name("ex:unqualified_name")

    namespaces = dict()
    namespaces.update({"ex": "http://example.com"})
    namespaces.update({"custom": "http://custom.com"})
    namespaces.update({"prov": "http://www.w3.org/ns/prov#"})

    type_map = dict()
    type_map.update({"ex:date value": {"type": "xsd:dateTime"}})
    type_map.update({"ex:double value": {"type": "xsd:double"}})
    type_map.update({"ex:int value": {"type": "xsd:int"}})

    type_map.update({"ex:Qualified name ": {'type': 'prov:QUALIFIED_NAME'}})
    # type_map.update({"ex:Qualified name 2":{'type': 'prov:QUALIFIED_NAME'}}) #The prov lib don't require to auto convert strings into qualified names
    type_map.update({"ex:Literal": {'lang': 'en'}})
    type_map.update({"ex:Literal 2": {'lang': 'en'}})
    type_map.update({"ex:identifier type": {'type': 'xsd:anyURI'}})

    metadata = dict()
    metadata.update({METADATA_KEY_PROV_TYPE: PROV_RECORD_IDS_MAP["activity"]})
    metadata.update({METADATA_KEY_IDENTIFIER: doc.valid_qualified_name("ex:record")})
    metadata.update({METADATA_KEY_NAMESPACES: namespaces})
    metadata.update({METADATA_KEY_TYPE_MAP: type_map})

    record = ProvActivity(doc, "ex:record", attributes)
    # noinspection PyPep8Naming
    Example = namedtuple("prov_api_metadata_record_example", "metadata, attributes, prov_record, expected_attributes")

    return Example(metadata, attributes, record, expected_attributes)
