import asyncio
from concurrent import futures
from rx import Observable

from provdbconnector.prov_db import ProvDb
from provdbconnector.utils.converter import form_string
from provdbconnector import Neo4jAdapter, NEO4J_USER, NEO4J_PASS, NEO4J_HOST, NEO4J_BOLT_PORT
import pkg_resources
import time
auth_info = {"user_name": NEO4J_USER,
                  "user_password": NEO4J_PASS,
                  "host": NEO4J_HOST + ":" + NEO4J_BOLT_PORT
                  }


# create the prov document from examples
prov_document_buffer = pkg_resources.resource_stream("examples", "horsemeat_example.json")

# Save document
doc  = form_string(prov_document_buffer)

provapi = ProvDb(adapter=Neo4jAdapter, auth_info=auth_info)

def runAsync():
    event_loop = asyncio.get_event_loop()
    try:
        print ( "START SAVE ")
        event_loop.run_until_complete(provapi.save_document(doc))
    finally:
        event_loop.close()



runAsync()