import unittest

def setUpModule():
    pass

def tearDownModule():
    pass

class TestSimpleExample(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    def test_joke(self):
        from data_lineage.dataset_lineage_mgmt import a_joke

        self.assertEqual(a_joke(), "I said something funny")


if __name__ == '__main__':
    unittest.main()