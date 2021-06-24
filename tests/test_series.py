import os
import unittest


class TestSeries(unittest.TestCase):

    @unittest.skip('bug')
    def test_write_series_append_after_close(self):
        from tempsdb.series import create_series, TimeSeries
        series = create_series('test6', 'test6', 10, 4096)

        for i in range(8000):
            series.append(i, b'\x00'*10)

        series.close()
        series = TimeSeries('test6', 'test6')
        for i in range(8000, 16000):
            series.append(i, b'\x00'*10)

        cur_val = 0
        with series.iterate_range(0, 17000) as it:
            for ts, v in it:
                if ts != cur_val:
                    self.fail('Failed at %s:%s' % (ts, cur_val))
                cur_val += 1

        series.close()

    @unittest.skip('because of reasons')
    def test_write_series_with_interim_close(self):
        from tempsdb.series import create_series, TimeSeries
        series = create_series('test4', 'test4', 10, 4096)

        self.assertRaises(ValueError, series.get_current_value)
        for i in range(8000):
            series.append(i, b'\x00'*10)

        series.close()
        series = TimeSeries('test4', 'test4')
        self.assertEqual(series.last_entry_ts, i)
        self.assertEqual(series.get_current_value(), (i, b'\x00'*10))

        with series.iterate_range(i, i) as it:
            lst = list(it)
            self.assertEqual(len(lst), 1)
            self.assertEqual(lst[0][0], i)

        series.trim(4100)

        self.assertEqual(len(os.listdir('test3')), 2)
        series.close()

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
