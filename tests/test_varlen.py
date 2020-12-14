import os
import unittest


class TestVarlen(unittest.TestCase):
    def test_varlen(self):
        from tempsdb.varlen import create_varlen_series

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

    def test_varlen_gzip(self):
        from tempsdb.varlen import create_varlen_series

        series = [(0, b'test skarabeusza'), (10, b'test skarabeuszatest skarabeusza')]
        varlen = create_varlen_series('test_dir.gz', 'test_dir.gz', 2, [10, 20, 10], 20,
                                      gzip_level=1)

        varlen.append(*series[0])
        self.assertEqual(len(os.listdir('test_dir.gz')), 2)

        varlen.append(*series[1])
        self.assertEqual(len(os.listdir('test_dir.gz')), 3)

        it = varlen.iterate_range(0, 20)
        lst = [(ts, v.to_bytes()) for ts, v in it]
        it.close()
        self.assertEqual(lst, series)
