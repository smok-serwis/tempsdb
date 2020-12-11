import logging
import os
import unittest

from tempsdb.varlen import create_varlen_series

logger = logging.getLogger(__name__)


class TestVarlen(unittest.TestCase):
    def test_varlen(self):
        series = [(0, b'test skarabeusza'), (10, b'test skarabeuszatest skarabeusza')]
        varlen = create_varlen_series('test_dir', 'test_dir', 2, [10, 20, 10], 20)

        varlen.append(*series[0])
        self.assertEqual(len(os.listdir('test_dir')), 2)

        varlen.append(*series[1])
        self.assertEqual(len(os.listdir('test_dir')), 3)

        it = varlen.iterate_range(0, 20)
        lst = [(ts, v.to_bytes()) for ts, v in it]
        it.close()
        self.assertEqual(lst, series)
