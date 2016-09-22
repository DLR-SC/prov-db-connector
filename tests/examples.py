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


def base_connector_record_parameter_example():

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

    metadata.update({"type": "prov:Activity"})
    metadata.update({"label": "label for the node"})
    metadata.update({"type_map": type_map})
    metadata.update({"namespaces": namespaces})


    return_data = dict()
    return_data.update({"attributes": attributes})
    return_data.update({"metadata": metadata})

    return return_data



def base_connector_relation_parameter_example():

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

    metadata.update({"type": "mentionOf"})
    metadata.update({"label": "label for the node"})
    metadata.update({"type_map": type_map})
    metadata.update({"namespaces": namespaces})


    return_data = dict()
    return_data.update({"attributes": attributes})
    return_data.update({"metadata": metadata})

    return return_data



