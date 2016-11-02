import unittest


class ExamplesTest(unittest.TestCase):

    def test_bundle_example(self):
        import examples.bundle_example


    def test_test_complex_example_with_neo4j(self):
        import examples.complex_example_with_neo4j


    def test_file_buffer_example(self):
        import examples.file_buffer_example


    def test_simple_example(self):
        import examples.simple_example

    def test_simple_example_with_neo4j(self):
        import examples.simple_example_with_neo4j

    def test_merge_example(self):
        import examples.merge_example

    def test_merge_fail_example(self):
        import examples.merge_fail_example