from prov.model import ProvDocument
from provdbconnector import ProvDb
from provdbconnector import Neo4jAdapter
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

# create the prov document
prov_document = ProvDocument()
prov_document.add_namespace("ex", "http://example.com")

prov_document.agent("ex:Bob")
prov_document.activity("ex:Alice")

prov_document.association("ex:Alice", "ex:Bob")

document_id = prov_api.save_document(prov_document)

print(prov_api.get_document_as_provn(document_id))

# Output:
#
# document
# prefix
# ex < http: // example.com >
#
# agent(ex:Bob)
# activity(ex:Alice, -, -)
# wasAssociatedWith(ex:Alice, ex:Bob, -)
# endDocument
