#!/bin/bash

#wait until server is up
until $(curl --output /dev/null --silent --head --fail http://neo4j-server:7474); do
    printf '.'
    sleep 5
done

curl -vX POST http://neo4j:neo4j@neo4j-server:7474/user/neo4j/password -d"password=neo4jneo4j";

python setup.py test