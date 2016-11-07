.. _test_howto:

Testing Howto
-------------
To run the test local follow the next steps

1. Setup your env
~~~~~~~~~~~~~~~~~

.. include::  ./development.rst
    :start-after: Setup
    :end-before: Execute tests

2. Start your neo4j setup
~~~~~~~~~~~~~~~~~~~~~~~~~

The tests require a running neo4j 3.0+ instance
The simples way do start neo4j ist to use the docker image provided by neo4j

.. code:: sh

    docker run \
        --publish=7474:7474 --publish=7687:7687 \
        --volume=$HOME/neo4j/data:/data \
        neo4j:3.0


Then open a browser `http://localhost:7474` and set the password to **neo4jneo4j**
Alternative you can set the env. variables:

- NEO4J_USERNAME: Default: neo4j
- NEO4J_PASSWORD: Default: neo4jneo4j
- NEO4J_HOST: Default: localhost
- NEO4J_BOLT_PORT: Default: 7687
- NEO4J_HTTP_PORT: Default: 7474


3. Run your tests
~~~~~~~~~~~~~~~~~

.. code:: sh

    # Change env
    source env/bin/activate
    #Start tests
    make test