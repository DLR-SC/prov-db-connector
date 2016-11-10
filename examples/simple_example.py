from prov.model import ProvDocument
from provdbconnector import ProvDb
from provdbconnector.db_adapters.in_memory import SimpleInMemoryAdapter

prov_api = ProvDb(adapter=SimpleInMemoryAdapter, auth_info=None)

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
