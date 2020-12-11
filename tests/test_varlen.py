import os
import unittest

from tempsdb.varlen import create_varlen_series


class TestVarlen(unittest.TestCase):
    def test_varlen(self):
        varlen = create_varlen_series('test_dir', 'test_dir', 2, [10, 20, 10], 20)
        try:
            varlen.append(0, b'test skarabeusza')
            self.assertEqual(len(os.listdir('test_dir')), 2)

            varlen.append(10, b'test skarabeuszatest skarabeusza')
            self.assertEqual(len(os.listdir('test_dir')), 3)
        finally:
            varlen.close()
