from provdbconnector import ProvDb
from provdbconnector import Neo4jAdapter
from prov.tests.examples import primer_example
import os

# create the api

NEO4J_USER = os.environ.get('NEO4J_USERNAME', 'neo4j')
NEO4J_PASS = os.environ.get('NEO4J_PASSWORD', 'neo4jneo4j')
NEO4J_HOST = os.environ.get('NEO4J_HOST', 'localhost')
NEO4J_BOLT_PORT = os.environ.get('NEO4J_BOLT_PORT', '7687')

auth_info = {"user_name": NEO4J_USER,
             "user_password": NEO4J_PASS,
             "host": NEO4J_HOST + ":" + NEO4J_BOLT_PORT
             }

prov_api = ProvDb(adapter=Neo4jAdapter, auth_info=auth_info)

# create the prov document from examples
prov_document = primer_example()

# Save document
document_id = prov_api.save_document(prov_document)

# get document
print(prov_api.get_document_as_provn(document_id))

# Output:

# document
# prefix
# foaf < http: // xmlns.com / foaf / 0.1 / >
# prefix
# dcterms < http: // purl.org / dc / terms / >
# prefix
# ex < http: // example / >
#
# specializationOf(ex:articleV2, ex:article)
# specializationOf(ex:articleV1, ex:article)
# wasDerivedFrom(ex:blogEntry, ex:article, -, -, -, [prov:type = 'prov:Quotation'])
# alternateOf(ex:articleV2, ex:articleV1)
# wasDerivedFrom(ex:articleV1, ex:dataSet1, -, -, -)
# wasDerivedFrom(ex:articleV2, ex:dataSet2, -, -, -)
# wasDerivedFrom(ex:dataSet2, ex:dataSet1, -, -, -, [prov:type = 'prov:Revision'])
# used(ex:correct, ex:dataSet1, -)
# used(ex:compose, ex:dataSet1, -, [prov:role = "ex:dataToCompose"])
# wasDerivedFrom(ex:chart2, ex:dataSet2, -, -, -)
# wasGeneratedBy(ex:dataSet2, ex:correct, -)
# used(ex:compose, ex:regionList, -, [prov:role = "ex:regionsToAggregateBy"])
# used(ex:illustrate, ex:composition, -)
# wasGeneratedBy(ex:composition, ex:compose, -)
# wasAttributedTo(ex:chart1, ex:derek)
# wasGeneratedBy(ex:chart1, ex:compile, 2012 - 03 - 02
# T10:30:00)
# wasGeneratedBy(ex:chart1, ex:illustrate, -)
# wasAssociatedWith(ex:compose, ex:derek, -)
# wasAssociatedWith(ex:illustrate, ex:derek, -)
# actedOnBehalfOf(ex:derek, ex:chartgen, ex:compose)
# entity(ex:article, [dcterms:title = "Crime rises in cities"])
# entity(ex:articleV1)
# entity(ex:articleV2)
# entity(ex:dataSet1)
# entity(ex:dataSet2)
# entity(ex:regionList)
# entity(ex:composition)
# entity(ex:chart1)
# entity(ex:chart2)
# entity(ex:blogEntry)
# activity(ex:compile, -, -)
# activity(ex:compile2, -, -)
# activity(ex:compose, -, -)
# activity(ex:correct, 2012 - 03 - 31
# T09:21:00, 2012 - 04 - 01
# T15:21:00)
# activity(ex:illustrate, -, -)
# agent(ex:derek, [foaf:mbox = "<mailto:derek@example.org>", foaf:givenName = "Derek", prov:type = 'prov:Person'])
# agent(ex:chartgen, [foaf:name = "Chart Generators Inc", prov:type = 'prov:Organization'])
# endDocument
