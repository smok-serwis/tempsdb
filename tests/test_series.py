import os
import unittest


class TestSeries(unittest.TestCase):
    def test_write_series(self):
        from tempsdb.series import create_series
        series = create_series('test3', 'test3', 10, 4096)

        self.assertRaises(ValueError, series.get_current_value)

        for i in range(8000):
            series.append(i, b'\x00'*10)

        self.assertEqual(series.get_current_value(), (i, b'\x00'*10))

        with series.iterate_range(i, i) as it:
            lst = list(it)
            self.assertEqual(len(lst), 1)
            self.assertEqual(lst[0][0], i)

        series.trim(4100)

        self.assertEqual(len(os.listdir('test3')), 2)
        series.close()

    def test_create_series(self):
        from tempsdb.series import create_series

        series = create_series('test', 'test', 1, 10)
        start, ts = 127, 100
        for i in range(20):
            series.append(ts, bytes(bytearray([start])))
            start -= 1
            ts += 100

        self.do_verify_series(series, 0, 2000)
        self.do_verify_series(series, 500, 2000)
        self.do_verify_series(series, 1000, 2000)
        self.do_verify_series(series, 1500, 2000)
        self.do_verify_series(series, 0, 500)
        self.do_verify_series(series, 0, 1200)
        self.do_verify_series(series, 0, 1800)
        series.close()

    def test_create_series_gzip(self):
        from tempsdb.series import create_series

        series = create_series('test.gz', 'test.gz', 1, 10, gzip_level=6)
        start, ts = 127, 100
        for i in range(20):
            series.append(ts, bytes(bytearray([start])))
            start -= 1
            ts += 100

        self.do_verify_series(series, 0, 2000)
        self.do_verify_series(series, 500, 2000)
        self.do_verify_series(series, 1000, 2000)
        self.do_verify_series(series, 1500, 2000)
        self.do_verify_series(series, 0, 500)
        self.do_verify_series(series, 0, 1200)
        self.do_verify_series(series, 0, 1800)
        series.close()

    def do_verify_series(self, series, start, stop):
        it = series.iterate_range(start, stop)
        items = list(it)
        it.close()
        self.assertGreaterEqual(items[0][0], start)
        self.assertLessEqual(items[-1][0], stop)
