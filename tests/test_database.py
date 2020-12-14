import unittest

from tempsdb.database import create_database


class TestDatabase(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.db = create_database('my_db')

    def test_add_series(self):
        ser = self.db.create_series('hello-world', 1, 10)
        ser.append(10, b'\x00')
        ser.append(20, b'\x00')
        ser.close()

        ser = self.db.get_series('hello-world')
        self.assertEqual(ser.last_entry_ts, 20)
        ser.close()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.db.close()
