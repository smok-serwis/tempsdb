import unittest

from tempsdb.chunks import create_chunk


class TestDB(unittest.TestCase):
    def test_chunk(self):
        data = [(0, b'ala '), (1, b'ma  '), (4, b'kota')]
        chunk = create_chunk('chunk.db', data)
        self.assertEqual(chunk.min_ts, 0)
        self.assertEqual(chunk.max_ts, 4)
        self.assertEqual(chunk.bs, 4)
        self.assertEqual(chunk.get_piece_at(0), (0, b'ala '))
        self.assertEqual(chunk.get_piece_at(1), (1, b'ma '))
        self.assertEqual(chunk.get_piece_at(2), (4, b'kota'))
