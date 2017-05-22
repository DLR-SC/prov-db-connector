#!/bin/bash
while ! nc -z "$NEO4J_HOST" "$NEO4J_REST_PORT"; do sleep 3; done

url="http://$NEO4J_HOST:$NEO4J_REST_PORT/user/neo4j/password"

printf "$url"

curl -H "Content-Type: application/json" -X POST -d '{"password":"neo4jneo4j"}' -u neo4j:neo4j "$url"

args=
for arg in "$@";
do
  args="$args '$arg'"
done
eval exec $args