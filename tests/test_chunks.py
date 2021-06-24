import os
import unittest


class TestDB(unittest.TestCase):

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
        self.assertEqual(os.path.getsize('chunk.db'), 4096)

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
        self.assertEqual(os.path.getsize('chunk.db'), 4096)
