import unittest
from xml.etree import ElementTree
import pkg_resources
import json

from prov.model import ProvDocument
from prov.tests import examples
from provdbconnector.utils import to_json, from_json, to_provn, from_provn, to_xml, from_xml
from provdbconnector.utils import NoDocumentException


class ConverterTests(unittest.TestCase):

    def setUp(self):
        # Reading testfiles from prov package according to:
        # http://stackoverflow.com/questions/6028000/python-how-to-read-a-static-file-from-inside-a-package
        #
        #   Assuming your template is located inside your module's package at this path:
        #   <your_package>/templates/temp_file
        #   the correct way to read your template is to use pkg_resources package from setuptools distribution:
        test_resources = {
            'xml': {'package': 'provdbconnector', 'file': '../tests/resources/primer.provx'},
            'json':{'package':'provdbconnector', 'file':'../tests/resources/primer.json'},
            'provn':{'package':'provdbconnector', 'file':'../tests/resources/primer.provn'}
        }
        self.test_files = dict( (key, pkg_resources.resource_stream(val['package'], val['file'])) for key, val in test_resources.items())
        self.prov_document = examples.primer_example()

    def tearDown(self):
        pass

    def test_to_json(self):
        self.assertRaises(NoDocumentException, lambda: to_json())
        json_document = to_json(self.prov_document)
        self.assertIsInstance(json_document, str)
        try:
            json.loads(json_document)
        except ValueError:
            self.fail("Invalid JSON")

    def test_from_json(self):
        self.assertRaises(NoDocumentException, lambda: from_json())
        prov = from_json(self.test_files['json'])
        self.assertIsInstance(prov, ProvDocument)

    def test_to_provn(self):
        self.assertRaises(NoDocumentException, lambda: to_provn())
        provn_document = to_provn(self.prov_document)
        self.assertIsInstance(provn_document, str)
        #Validate that string is in provn format

    def test_from_provn(self):
        self.assertRaises(NoDocumentException, lambda: from_provn())
        prov = from_provn(self.test_files['provn'])
        self.assertIsInstance(prov, ProvDocument)

    def test_to_xml(self):
        self.assertRaises(NoDocumentException, lambda: to_xml())
        xml_document = to_xml(self.prov_document)
        self.assertIsInstance(xml_document, str)
        ElementTree.fromstring(xml_document)

    def test_from_xml(self):
        self.assertRaises(NoDocumentException, lambda: from_xml())
        prov = from_xml(self.test_files['xml'])
        self.assertIsInstance(prov, ProvDocument)
