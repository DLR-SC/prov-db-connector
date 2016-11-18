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

def doWork(element):
    provapi = ProvDb(adapter=Neo4jAdapter, auth_info=auth_info)
    provapi.save_record(element)


async def phase(i,executor):
    #print('in phase {}'.format(i))
    job = executor.submit(doWork, i)
    return (await asyncio.wrap_future(job))
    #print('done with phase {}'.format(i))
    #return 'phase {} result'.format(i)


async def main(num_phases):
    print('starting main')

    xs = Observable.from_(doc.bundles)
    xs = xs.flat_map(lambda x: x.get_records())
    xs = xs.to_blocking()

    with futures.ProcessPoolExecutor() as executor:

        phases = [
            phase(i,executor)
            for i in xs
            ]
        print('waiting for phases to complete')
        start_time = time.time()
        completed, pending = await asyncio.wait(phases)
        print("--- %s seconds ---" % (time.time() - start_time))
#    results = [t.result() for t in completed]
#    print('results: {!r}'.format(results))

def runAsync():
    event_loop = asyncio.get_event_loop()
    try:
        event_loop.run_until_complete(main(3))
    finally:
        event_loop.close()


def runSync():
    xs = Observable.from_(doc.bundles)
    xs = xs.flat_map(lambda x: x.get_records())
    xs = xs.to_blocking()


    print('waiting for phases to complete')
    start_time = time.time()
    provapi = ProvDb(adapter=Neo4jAdapter, auth_info=auth_info)
    for record in xs:
        provapi.save_record(record)
    print("--- %s seconds ---" % (time.time() - start_time))


runAsync()