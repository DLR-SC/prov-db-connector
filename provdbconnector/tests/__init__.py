from provdbconnector.tests.db_adapters.test_baseadapter import AdapterTestTemplate
from provdbconnector.tests.test_provDb import ProvDbTestTemplate
import unittest


def additional_tests():
    from examples.tests.test_examples import ExamplesTest
    return unittest.defaultTestLoader.loadTestsFromTestCase(ExamplesTest)

