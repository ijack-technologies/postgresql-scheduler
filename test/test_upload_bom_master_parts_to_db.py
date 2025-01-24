import sys
import unittest

# Insert pythonpath into the front of the PATH environment variable, before importing anything from project/
pythonpath = "/workspace"
try:
    sys.path.index(pythonpath)
except ValueError:
    sys.path.insert(0, pythonpath)


from project.upload_bom_master_parts_to_db import clean_part_number


class TestCleanPartNumber(unittest.TestCase):
    def test_integers(self):
        self.assertEqual(clean_part_number(123), "123")
        self.assertEqual(clean_part_number(0), "0")

    def test_floats(self):
        self.assertEqual(clean_part_number(123.0), "123")
        self.assertEqual(clean_part_number(123.45), "123.45")

    def test_scientific(self):
        self.assertEqual(clean_part_number(1.2e5), "120000")
        self.assertEqual(clean_part_number("1.2e5"), "1.2e5")

    def test_strings(self):
        self.assertEqual(clean_part_number("123"), "123")
        self.assertEqual(clean_part_number("007"), "007")
        self.assertEqual(clean_part_number(" 123 "), "123")
        self.assertEqual(clean_part_number(""), "")


if __name__ == "__main__":
    unittest.main()
