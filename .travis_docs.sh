#!/usr/bin/env bash

sphinx-apidoc -o docs provdbconnector
sphinx-build -q -a -b html -d docs/build/doctrees docs/ docs/build/html &> travis-doc-test.txt
TEST=$(grep 'failed' travis-doc-test.txt | LC_ALL=C.UTF-8 wc -m)
echo "Lenght of errors = $TEST"
if test $TEST -gt 0
then
    echo "Error during build docs "
    grep 'failed' travis-doc-test.txt
    exit 2
else
    echo "Build docs complete"
fi
