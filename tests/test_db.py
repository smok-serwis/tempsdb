import os
import unittest
from tempsdb.chunks import create_chunk
from tempsdb.series import create_series


class TestDB(unittest.TestCase):

    def test_create_series(self):
        series = create_series('test', 8, 10)

    def test_chunk(self):
        data = [(0, b'ala '), (1, b'ma  '), (4, b'kota')]
        chunk = create_chunk(None, 'chunk.db', data)
        self.assertEqual(chunk.min_ts, 0)
        self.assertEqual(chunk.max_ts, 4)
        self.assertEqual(chunk.block_size, 4)
        self.assertEqual(chunk.get_piece_at(0), (0, b'ala '))
        self.assertEqual(chunk.get_piece_at(1), (1, b'ma  '))
        self.assertEqual(chunk.get_piece_at(2), (4, b'kota'))
        self.assertEqual(len(chunk), 3)
        self.assertEqual(list(iter(chunk)), data)
        chunk.put(5, b'test')
        chunk.close()
        self.assertEqual(os.path.getsize('chunk.db'), 4+4*12)
