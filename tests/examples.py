#from prov.tests.examples import primer_example,\
#    primer_example_alternate,\
#    w3c_publication_1,\
#    w3c_publication_2,\
#    bundles1,\
#    bundles2,\
#    collections,\
#    long_literals,\
#    datatypes
import datetime
from prov.model import ProvDocument, QualifiedName
from prov.constants import PROV_RECORD_IDS_MAP
from provdbconnector.databases.baseadapter import METADATA_KEY_BUNDLE_ID, METADATA_KEY_NAMESPACES,METADATA_KEY_PROV_TYPE,METADATA_KEY_TYPE_MAP,METADATA_KEY_LABEL


def base_connector_bundle_parameter_example():
    doc = ProvDocument()
    attributes = dict()

    namespaces = dict()
    namespaces.update({"ex": "http://example.com"})

    type_map = dict()
    type_map.update({"int value": "int"})
    type_map.update({"date value": "xds:datetime"})

    metadata = dict()

    metadata.update({METADATA_KEY_PROV_TYPE: doc.valid_qualified_name("prov:Bundle")})
    metadata.update({METADATA_KEY_LABEL: "label for the node"})
    metadata.update({METADATA_KEY_TYPE_MAP: type_map})
    metadata.update({METADATA_KEY_NAMESPACES: namespaces})

    return_data = dict()
    return_data.update({"attributes": attributes})
    return_data.update({"metadata": metadata})
    return return_data


def base_connector_record_parameter_example():
    doc = ProvDocument()
    attributes = dict()
    attributes.update({"individual attribute": "Some value"})
    attributes.update({"int value": 99})
    attributes.update({"double value": 99.33})
    attributes.update({"date value": datetime.datetime.now()})


    namespaces = dict()
    namespaces.update({"ex": "http://example.com"})
    namespaces.update({"custom": "http://custom.com"})

    type_map = dict()
    type_map.update({"int value": "int"})
    type_map.update({"date value": "xds:datetime"})


    metadata  = dict()

    metadata.update({METADATA_KEY_PROV_TYPE: doc.valid_qualified_name("prov:Activity")})
    metadata.update({METADATA_KEY_LABEL: "label for the node"})
    metadata.update({METADATA_KEY_TYPE_MAP: type_map})
    metadata.update({METADATA_KEY_NAMESPACES: namespaces})


    return_data = dict()
    return_data.update({"attributes": attributes})
    return_data.update({"metadata": metadata})

    return return_data



def base_connector_relation_parameter_example():
    doc = ProvDocument()
    doc.add_namespace("ex", "http://example.com")
    doc.add_namespace("custom", "http://custom.com")

    attributes = dict()
    attributes.update({"individual attribute": "Some value"})
    attributes.update({"int value": 99})
    attributes.update({"double value": 99.33})
    attributes.update({"date value": datetime.datetime.now()})


    namespaces = dict()
    namespaces.update({"ex": "http://example.com"})
    namespaces.update({"custom": "http://custom.com"})

    type_map = dict()
    type_map.update({"int value": "int"})
    type_map.update({"date value": "xds:datetime"})


    metadata  = dict()

    metadata.update({METADATA_KEY_PROV_TYPE: PROV_RECORD_IDS_MAP["mentionOf"]})
    metadata.update({METADATA_KEY_LABEL: "label for the relation"})
    metadata.update({METADATA_KEY_TYPE_MAP: type_map})
    metadata.update({METADATA_KEY_NAMESPACES: namespaces})


    return_data = dict()
    return_data.update({"attributes": attributes})
    return_data.update({"metadata": metadata})
    return_data.update({"from_node": doc.valid_qualified_name("ex:Yoda")})
    return_data.update({"to_node": doc.valid_qualified_name("ex:Luke Skywalker")})
    return_data.update({"doc": doc})

    return return_data



