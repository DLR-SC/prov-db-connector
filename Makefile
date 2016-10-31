.PHONY: clean-pyc clean-build docs clean

help:
	@echo "setup - basic setup and install"
	@echo "dev-setup - basic setup for developers"
	@echo "clean-build - remove build artifacts"
	@echo "clean-pyc - remove Python file artifacts"
	@echo "test - run tests quickly with the default Python"
	@echo "coverage - check code coverage quickly with the default Python"
	@echo "docs - generate Sphinx HTML documentation, including API docs"
	@echo "release - package and upload a release"
	@echo "dist - package"

setup:
	pip install -U pip setuptools
	pip install '.'

dev-setup:
	mkdir docs/source/_static
	pip install -U pip setuptools
	pip install -e '.[dev]'

clean: clean-build clean-pyc
	rm -fr htmlcov/

clean-build:
	rm -fr build/
	rm -fr dist/
	rm -fr *.egg-info

clean-pyc:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +

test:
	python setup.py test

coverage:
	coverage run --source provdbconnector setup.py test
	coverage report -m
	coverage html

docs:
	rm -f docs/prov.rst
	rm -f docs/modules.rst
	which sphinx-apidoc
	sphinx-apidoc -o docs/source provdbconnector
	$(MAKE) -C docs clean
	$(MAKE) -C docs html

release: clean
	python setup.py sdist upload

dist: clean
	python setup.py sdist
