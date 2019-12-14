import unittest


class ExamplesTest(unittest.TestCase):
    """
    This test is only to load the example and check if the examples are still running

    """

    def test_bundle_example(self):
        """
        Test the bundle example

        """
        import examples.bundle_example

    def test_test_complex_example_with_neo4j(self):
        """
        Test the neo4j example

        """
        import examples.complex_example_with_neo4j

    def test_file_buffer_example(self):
        """
        Test the file buffer example

        """
        import examples.file_buffer_example

    def test_simple_example(self):
        """
        Test the basic example

        """
        import examples.simple_example

    def test_simple_example_influence(self):
        """
        Test the basic example with influence by relation

        """
        import examples.simple_example_influence

    def test_simple_example_with_neo4j(self):
        """
        Test the basic neo4j example

        """
        import examples.simple_example_with_neo4j

    def test_merge_example(self):
        """
        Test the merge example

        """
        import examples.merge_example

    def test_merge_fail_example(self):
        """
        Test the merge fail example

        """
        import examples.merge_fail_example

    def test_horsemeat_example(self):
        """
        Test the merge fail example
        """
        import examples.horsemeat_example