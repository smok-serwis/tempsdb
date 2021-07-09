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
        varlen.sync()
        it = varlen.iterate_range(0, 20)
        lst = [(ts, v.to_bytes()) for ts, v in it]
        it.close()
        self.assertEqual(lst, series)

    def test_varlen_iterator(self):
        from tempsdb.varlen import create_varlen_series

        series = [(0, b'test skarabeusza'), (10, b'test skarabeuszatest skarabeusza')]
        varlen = create_varlen_series('test2_dir', 'test2_dir', 2, [10, 20, 10], 20)

        varlen.append(*series[0])
        varlen.append(*series[1])

        self.assertEqual(varlen.last_entry_ts, 10)

        with varlen.iterate_range(0, 20) as iterator:
            ve = iterator.get_next()
            while ve is not None:
                self.assertTrue(ve.startswith(b'test '))
                self.assertTrue(ve.endswith(b'skarabeusza'))
                self.assertEqual(ve.get_byte_at(3), ord('t'))
                self.assertFalse(ve.startswith(b'tost'))
                self.assertTrue(ve.slice(0, 4), b'test')
                self.assertFalse(ve.endswith(b'skerabeusza'))
                self.assertGreater(ve, b'tes')
                self.assertLess(ve, b'tez')
                self.assertGreaterEqual(ve, b'tes')
                self.assertTrue(ve)
                self.assertLessEqual(ve, b'tez')
                hash(ve)
                self.assertNotEqual(ve, b'test')
                self.assertTrue(ve.startswith(b'test '))
                self.assertTrue(ve.endswith(b'skarabeusza'))
                self.assertEqual(ve.get_byte_at(3), ord('t'))
                self.assertFalse(ve.startswith(b'tost'))
                self.assertTrue(ve.slice(0, 4), b'test')
                self.assertFalse(ve.endswith(b'skerabeusza'))
                ve = iterator.get_next()

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
