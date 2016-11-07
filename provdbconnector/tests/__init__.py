import unittest

def additional_tests():
    from examples.tests.test_examples import ExamplesTest
    return unittest.defaultTestLoader.loadTestsFromTestCase(ExamplesTest)

