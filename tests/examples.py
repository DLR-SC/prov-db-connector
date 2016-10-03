from prov.tests.examples import primer_example,\
   primer_example_alternate,\
   w3c_publication_1,\
   w3c_publication_2,\
   bundles1,\
   bundles2,\
   collections,\
   long_literals,\
   datatypes
import datetime
from prov.model import ProvDocument, QualifiedName, ProvRecord, ProvRelation, ProvActivity, Literal, Identifier
from prov.constants import PROV_RECORD_IDS_MAP,PROV
from provdbconnector.db_adapters.baseadapter import METADATA_KEY_BUNDLE_ID, METADATA_KEY_NAMESPACES,METADATA_KEY_PROV_TYPE,METADATA_KEY_TYPE_MAP,METADATA_KEY_IDENTIFIER
from collections import namedtuple
import pkg_resources

test_resources = {
    'xml': {'package': 'provdbconnector', 'file': '../tests/resources/primer.provx'},
    'json': {'package': 'provdbconnector', 'file': '../tests/resources/primer.json'},
    'provn': {'package': 'provdbconnector', 'file': '../tests/resources/primer.provn'}
}
test_prov_files = dict(
    (key, pkg_resources.resource_stream(val['package'], val['file'])) for key, val in test_resources.items())



def attributes_dict_example():
    attributes = dict()
    attributes.update({"ex:individual attribute": "Some value"})
    attributes.update({"ex:int value": 99})
    attributes.update({"ex:double value": 99.33})
    attributes.update({"ex:date value": datetime.datetime.now()})
    attributes.update({"ex:list value": ["list","of","strings"]})
    attributes.update({"ex:dict value": {"dict":"value"}})

    return attributes

def base_connector_bundle_parameter_example():
    doc = ProvDocument()
    doc.add_namespace("ex", "http://example.com")
    attributes = dict()

    namespaces = dict()
    namespaces.update({"ex": "http://example.com"})

    type_map = dict()
    type_map.update({"int value": "int"})
    type_map.update({"date value": "xds:datetime"})

    metadata = dict()

    metadata.update({METADATA_KEY_PROV_TYPE: doc.valid_qualified_name("prov:Bundle")})
    metadata.update({METADATA_KEY_IDENTIFIER: doc.valid_qualified_name("ex:bundle name")})
    metadata.update({METADATA_KEY_TYPE_MAP: type_map})
    metadata.update({METADATA_KEY_NAMESPACES: namespaces})

    return_data = dict()
    return_data.update({"attributes": attributes})
    return_data.update({"metadata": metadata})
    return return_data


def base_connector_record_parameter_example():
    doc = ProvDocument()



    namespaces = dict()
    namespaces.update({"ex": "http://example.com"})
    namespaces.update({"custom": "http://custom.com"})

    type_map = dict()
    type_map.update({"int value": "int"})
    type_map.update({"date value": "xds:datetime"})


    metadata  = dict()

    metadata.update({METADATA_KEY_PROV_TYPE: doc.valid_qualified_name("prov:Activity")})
    metadata.update({METADATA_KEY_IDENTIFIER: "label for the node"})
    metadata.update({METADATA_KEY_TYPE_MAP: type_map})
    metadata.update({METADATA_KEY_NAMESPACES: namespaces})


    return_data = dict()
    return_data.update({"attributes": attributes_dict_example()})
    return_data.update({"metadata": metadata})

    return return_data



def base_connector_relation_parameter_example():
    doc = ProvDocument()
    doc.add_namespace("ex", "http://example.com")
    doc.add_namespace("custom", "http://custom.com")

    namespaces = dict()
    namespaces.update({"ex": "http://example.com"})
    namespaces.update({"custom": "http://custom.com"})

    type_map = dict()
    type_map.update({"int value": "int"})
    type_map.update({"date value": "xds:datetime"})


    metadata  = dict()

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



def prov_api_record_example():

    doc = ProvDocument()
    doc.add_namespace("ex", "http://example.com")
    doc.add_namespace("custom", "http://custom.com")

    attributes = attributes_dict_example()
    del attributes["ex:dict value"] #remove dict value because it is not allowed in a prov_record, but for low level adapter tests necessary
    del attributes["ex:list value"] #remove dict value because it is not allowed in a prov_record, but for low level adapter tests necessary
    attributes.update({"ex:Qualified name ": doc.valid_qualified_name("custom:qualified name")})
    attributes.update({"ex:Qualified name 2": "ex:unqualified_name"})
    attributes.update({"ex:Literral": Literal("test literral", langtag="en")})
    attributes.update({"ex:Literral 2": Literal("test literral with datatype", langtag="en", datatype=PROV["InternationalizedString"])})
    attributes.update({"ex:identifier type": Identifier("http://example.com/#test")})


    expected_attributes = dict()
    for key, value in attributes.items():
        new_key = doc.valid_qualified_name(key)
        expected_attributes.update({new_key: value})

    ##The prov lib don't require to auto convert string values into qualified names
    #valid_name = doc.valid_qualified_name("ex:Qualified name 2")
    #expected_attributes[valid_name] = doc.valid_qualified_name("ex:unqualified_name")


    namespaces = dict()
    namespaces.update({"ex": "http://example.com"})
    namespaces.update({"custom": "http://custom.com"})
    namespaces.update({"prov": "http://www.w3.org/ns/prov#"})

    type_map = dict()
    type_map.update({"ex:date value": {"type": "xsd:dateTime"}})
    type_map.update({"ex:double value": {"type": "xsd:double"}})
    type_map.update({"ex:int value":  {"type":"xsd:int"}})

    type_map.update({"ex:Qualified name ": {'type': 'prov:QUALIFIED_NAME'}})
    #type_map.update({"ex:Qualified name 2":{'type': 'prov:QUALIFIED_NAME'}}) #The prov lib don't require to auto convert strings into qualified names
    type_map.update({"ex:Literral": {'lang': 'en'}})
    type_map.update({"ex:Literral 2": {'lang': 'en'}})
    type_map.update({"ex:identifier type":{'type': 'xsd:anyURI'}})

    metadata = dict()
    metadata.update({METADATA_KEY_PROV_TYPE: PROV_RECORD_IDS_MAP["activity"]})
    metadata.update({METADATA_KEY_IDENTIFIER: doc.valid_qualified_name("ex:record")})
    metadata.update({METADATA_KEY_NAMESPACES: namespaces})
    metadata.update({METADATA_KEY_TYPE_MAP: type_map})

    record = ProvActivity(doc, "ex:record", attributes)
    Example = namedtuple("prov_api_metadata_record_example", "metadata, attributes, prov_record, expected_attributes")

    return Example(metadata,attributes,record,expected_attributes)
