import unittest

from tempsdb.database import create_database
from tempsdb.exceptions import DoesNotExist


class TestDatabase(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.db = create_database('my_db')

    def test_checkpoint(self):
        self.db.checkpoint()

    def test_metadata(self):
        meta = self.db.metadata
        self.assertFalse(self.db.metadata)
        meta = {'hello': 'world'}
        self.db.set_metadata(meta)
        self.db.reload_metadata()
        self.assertEqual(self.db.metadata, meta)

    def test_add_series(self):
        ser = self.db.create_series('hello-world', 1, 10)
        ser.append(10, b'\x00')
        ser.append(20, b'\x00')
        ser.close()

        ser = self.db.get_series('hello-world')
        self.assertEqual(ser.get_current_value(), (20, b'\x00'))
        self.assertEqual(ser.last_entry_ts, 20)
        ser.close()

        self.db.delete_series('hello-world')
        self.assertRaises(DoesNotExist, lambda: self.db.get_series('hello-world'))

    def test_add_varlen_series(self):
        ser = self.db.create_varlen_series('hello-world', [10, 20], 1, 20)
        ser.append(10, b'\x00')
        ser.append(20, b'\x00\x00\x00')
        ser.close()

        ser = self.db.get_varlen_series('hello-world')
        self.assertEqual(ser.get_current_value(), (20, b'\x00\x00\x00'))
        ser.close()

        self.db.delete_varlen_series('hello-world')
        self.assertRaises(DoesNotExist, lambda: self.db.get_varlen_series('hello-world'))

    @classmethod
    def tearDownClass(cls) -> None:
        cls.db.close()
