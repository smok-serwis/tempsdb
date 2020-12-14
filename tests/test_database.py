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
