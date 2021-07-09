import os
import unittest

from satella.json import write_json_to_file

from tempsdb.exceptions import Corruption


class TestSeries(unittest.TestCase):

    def test_trim_multiple_chunks_wo_close(self):
        """
        Test trimming after writing without closing the series.

        Also tests out close_chunks and open_chunks_mmap_size
        """
        from tempsdb.series import create_series, TimeSeries
        series = create_series('test7', 'test7', 10, 4096)

        for i in range(0, 16000):
            series.append(i, b'\x00'*10)
        self.assertGreaterEqual(series.open_chunks_mmap_size(), 4096)
        series.close_chunks()
        series.trim(8000)
        with series.iterate_range(0, 17000) as it:
            for ts, v in it:
                self.assertNotEqual(ts, 0)
        series.close()

    def test_corrupted_metadata(self):
        from tempsdb.series import create_series, TimeSeries
        series = create_series('test10', 'test10', 10, 4096)
        series.close()
        write_json_to_file('test10/metadata.txt', {})
        self.assertRaises(Corruption, TimeSeries('test10', 'test10'))

    def test_trim_multiple_chunks_with_close(self):
        from tempsdb.series import create_series, TimeSeries
        series = create_series('test8', 'test8', 10, 4096)

        for i in range(0, 16000):
            series.append(i, b'\x00'*10)
        series.sync()
        series.close()
        series = TimeSeries('test8', 'test8')
        series.trim(4097)
        with series.iterate_range(0, 17000) as it:
            for ts, v in it:
                self.assertNotEqual(ts, 0)
        series.close()

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

    def test_delete_series(self):
        from tempsdb.series import create_series, TimeSeries
        from tempsdb.exceptions import DoesNotExist

        series = create_series('test-delete', 'test-delete', 1, 10)
        start, ts = 127, 100
        for i in range(20):
            series.append(ts, bytes(bytearray([start])))
            start -= 1
            ts += 100

        series.delete()
        self.assertRaises(DoesNotExist, lambda: TimeSeries('test-delete', 'test-delete'))

    def test_disable_enable_mmap_sync(self):
        from tempsdb.series import create_series, TimeSeries

        series = create_series('test-mmap', 'test-mmap', 1, 10)
        start, ts = 127, 100
        for i in range(20):
            series.append(ts, bytes(bytearray([start])))
            start -= 1
            ts += 100

        series.sync()
        series.close()
        series = TimeSeries('test-mmap', 'test-mmap')
        series.disable_mmap()
        self.do_verify_series(series, 0, 2000)
        self.do_verify_series(series, 500, 2000)
        self.do_verify_series(series, 1000, 2000)
        self.do_verify_series(series, 1500, 2000)
        self.do_verify_series(series, 0, 500)
        self.do_verify_series(series, 0, 1200)
        self.do_verify_series(series, 0, 1800)
        series.enable_mmap()
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
