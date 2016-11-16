from prov.model import ProvDocument
from provdbconnector import ProvDb
from provdbconnector.exceptions.database import MergeException
from provdbconnector.db_adapters.in_memory import SimpleInMemoryAdapter

prov_api = ProvDb(adapter=SimpleInMemoryAdapter, auth_info=None)

# create the prov first document
first_prov_document = ProvDocument()
first_prov_document .add_namespace("ex", "http://example.com")

first_prov_document .agent("ex:Bob", other_attributes={"ex:last_name": "Meier"})
first_prov_document .activity("ex:Alice")

first_prov_document .association("ex:Alice", "ex:Bob")

first_document_id = prov_api.save_document(first_prov_document)

#Create the second prov document and merge the ex:Bob entry
second_prov_document = ProvDocument()
second_prov_document.add_namespace("ex", "http://example.com")

second_prov_document.agent("ex:Bob", other_attributes={"ex:age": 42, "ex:last_name": "Müller"})

try:
    second_document_id = prov_api.save_document(second_prov_document)
except MergeException as e:
    print ("Got the merge exception: {}".format(e))

# Query the first document ID and get only the last_name = Meier back. The merge was not successful.

print(prov_api.get_document_as_provn(first_document_id))

# Output:
#
# document
# prefix
# ex < http: // example.com >
#
# activity(ex:Alice, -, -)
# agent(ex:Bob, [ex:last_name = "Meier"])
# wasAssociatedWith(ex:Alice, ex:Bob, -)
# endDocument