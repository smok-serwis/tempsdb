import os
import unittest


class TestDB(unittest.TestCase):
    def test_write_series(self):
        from tempsdb.series import create_series
        series = create_series('test3', 'test3', 10, 4096)
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

    def test_chunk_alternative(self):
        from tempsdb.chunks.normal import NormalChunk
        from tempsdb.chunks.maker import create_chunk

        data = [(0, b'ala '), (1, b'ma  '), (4, b'kota')]
        chunk = create_chunk(None, 'chunk_a.db', 0, b'ala ', 4096)
        chunk.close()
        chunk = NormalChunk(None, 'chunk_a.db', 4096, use_descriptor_access=True)
        chunk.append(1, b'ma  ')
        chunk.append(4, b'kota')
        self.assertEqual(chunk.min_ts, 0)
        self.assertEqual(chunk.max_ts, 4)
        self.assertEqual(chunk.block_size, 4)
        self.assertEqual(chunk[0], (0, b'ala '))
        self.assertEqual(chunk[1], (1, b'ma  '))
        self.assertEqual(chunk[2], (4, b'kota'))
        self.assertEqual(len(chunk), 3)
        self.assertEqual(list(iter(chunk)), data)
        chunk.append(5, b'test')
        self.assertEqual(chunk.find_left(0), 0)
        self.assertEqual(chunk.find_left(1), 1)
        self.assertEqual(chunk.find_left(2), 2)
        self.assertEqual(chunk.find_left(3), 2)
        self.assertEqual(chunk.find_left(4), 2)
        self.assertEqual(chunk.find_left(5), 3)
        self.assertEqual(chunk.find_left(6), 4)
        self.assertEqual(chunk.find_right(0), 1)
        self.assertEqual(chunk.find_right(1), 2)
        self.assertEqual(chunk.find_right(2), 2)
        self.assertEqual(chunk.find_right(3), 2)
        self.assertEqual(chunk.find_right(4), 3)
        self.assertEqual(chunk.find_right(5), 4)
        self.assertEqual(chunk.find_right(6), 4)
        chunk.close()
        self.assertEqual(os.path.getsize('chunk.db'), 8192)

    def test_chunk(self):
        from tempsdb.chunks.maker import create_chunk

        data = [(0, b'ala '), (1, b'ma  '), (4, b'kota')]
        chunk = create_chunk(None, 'chunk.db', 0, b'ala ', 4096)
        chunk.append(1, b'ma  ')
        chunk.append(4, b'kota')
        self.assertEqual(chunk.min_ts, 0)
        self.assertEqual(chunk.max_ts, 4)
        self.assertEqual(chunk.block_size, 4)
        self.assertEqual(chunk[0], (0, b'ala '))
        self.assertEqual(chunk[1], (1, b'ma  '))
        self.assertEqual(chunk[2], (4, b'kota'))
        self.assertEqual(len(chunk), 3)
        self.assertEqual(list(iter(chunk)), data)
        chunk.append(5, b'test')
        self.assertEqual(chunk.find_left(0), 0)
        self.assertEqual(chunk.find_left(1), 1)
        self.assertEqual(chunk.find_left(2), 2)
        self.assertEqual(chunk.find_left(3), 2)
        self.assertEqual(chunk.find_left(4), 2)
        self.assertEqual(chunk.find_left(5), 3)
        self.assertEqual(chunk.find_left(6), 4)
        self.assertEqual(chunk.find_right(0), 1)
        self.assertEqual(chunk.find_right(1), 2)
        self.assertEqual(chunk.find_right(2), 2)
        self.assertEqual(chunk.find_right(3), 2)
        self.assertEqual(chunk.find_right(4), 3)
        self.assertEqual(chunk.find_right(5), 4)
        self.assertEqual(chunk.find_right(6), 4)
        chunk.close()
        self.assertEqual(os.path.getsize('chunk.db'), 8192)
