#!/usr/bin/env bash

sphinx-apidoc -o docs provdbconnector
sphinx-build -b html -d docs/build/doctrees docs/ docs/build/html -q -a &> test.txt
TEST=$(cat test.txt | grep 'failed' | LC_ALL=C.UTF-8 wc -m)
echo "Lenght of errors = $TEST"
if test $TEST -gt 0
then
    echo "Error during build docs "
    cat test.txt | grep 'failed'
    exit 2
else
    echo "Build docs complete"
fi