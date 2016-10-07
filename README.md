# PROV Database Connector

[![Build Status](https://travis-ci.org/DLR-SC/prov-db-connector.svg?branch=master)](https://travis-ci.org/DLR-SC/prov-db-connector) [![Coverage Status](https://coveralls.io/repos/github/DLR-SC/prov-db-connector/badge.svg?branch=master)](https://coveralls.io/github/DLR-SC/prov-db-connector?branch=master)[![Code Issues](https://www.quantifiedcode.com/api/v1/project/3ee099c99b0340728ca4d54392caae83/badge.svg)](https://www.quantifiedcode.com/app/project/3ee099c99b0340728ca4d54392caae83)



This python module provides a general interface to save [W3C-Prov](https://www.w3.org/TR/prov-overview/)-Documents into a database. 
Currently we support the [Neo4j](https://neo4j.com/)-Graph database.  

We transform a prov document into a graph structure and the result looks like this: 

![SVG-File of neo4j-graph](https://cdn.rawgit.com/DLR-SC/prov-db-connector/master/examples/complex_example_with_neo4j_graph.svg)


## Installation

**Not published yet**

```bash
pip install prov-db-connector
```

You can view [prov-db-connector on PyPi's package index](https://pypi.python.org/pypi/prov-db-connector/)

## Current status

### Todos / Next steps
* Documentation of classes and functions 
* Implement low-level functions in ProvApi like *create_record()*
* [Implement Tests: validator and validator class](https://github.com/DLR-SC/prov-db-connector/issues/1)


## Simple save / get prov document example

```python 

from prov.model import ProvDocument
from provdbconnector import ProvApi
from provdbconnector.db_adapters import SimpleInMemoryAdapter

prov_api = ProvApi(adapter=SimpleInMemoryAdapter,authinfo=None)



#create the prov document
prov_document = ProvDocument()
prov_document.add_namespace("ex", "http://example.com")

prov_document.agent("ex:Bob")
prov_document.activity("ex:Alice")

prov_document.association("ex:Alice","ex:Bob")

document_id = prov_api.create_document(prov_document)

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

````

## File Buffer example 

````python 
from prov.model import ProvDocument
from provdbconnector import ProvApi
from provdbconnector.db_adapters import SimpleInMemoryAdapter
from prov.tests.examples import primer_example
import pkg_resources

#create the api
prov_api = ProvApi(adapter=SimpleInMemoryAdapter,authinfo=None)


#create the prov document from examples
prov_document_buffer = pkg_resources.resource_stream("examples","file_buffer_example_primer.json")

#Save document
document_id = prov_api.create_document(prov_document_buffer)
#This is similar to:
# prov_api.create_document_from_json(prov_document_buffer)

#get document
print(prov_api.get_document_as_provn(document_id))

# Output:

....

````

You find all examples in the [examples](./examples) folder 


## Create your own database adapter 

It is also possible that you create your own custom pro-database adapater. To do this please follow the instrcutions in [Database-Adapters.md](./docs/Database-Adapaters.md)

## Contribute

- Issue Tracker: https://github.com/DLR-SC/prov-db-connector/issues
- Source Code: https://github.com/DLR-SC/prov-db-connector


## License

See [LICENSE](./LICENSE) file
