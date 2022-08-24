import unittest
from parameterized import parameterized
from tap_sftp import discover

class JSONLIterator:
    def __init__(self, data):
        self.data = data

    def decode(self, encoding):
        return self.data

class Iterable:
    def __init__(self, data):
        self.data = data

    def read(self):
        return self.data

class TestCheckJSONLKeyProperties(unittest.TestCase):
    """Test cases to verify we raise error if we asr missing Primary Key or Date Overrides value in JSONL data"""
    def test_get_JSONL_iterators_positive(self):
        options = {
            "key_properties": ["id"],
            "date_overrides": ["updated_at"]
        }
        records = [
            JSONLIterator('{"id": 1, "name": "abc", "updated_at": "2022-01-01"}')
        ]
        discover.get_JSONL_iterators(
            options=options,
            iterator=records
        )

    @parameterized.expand([
        ["raise_key_properties_error", '{"name": "abc", "updated_at": "2022-01-01"}', "CSV file missing required headers: {\'id\'}"],
        ["raise_date_overrides_error", '{"id": 1, "name": "abc"}', "CSV file missing date_overrides headers: {\'updated_at\'}"]
    ])
    def test_get_JSONL_iterators(self, name, test_data, expected_data):
        options = {
            "key_properties": ["id"],
            "date_overrides": ["updated_at"]
        }
        records = [
            JSONLIterator(test_data)
        ]
        with self.assertRaises(Exception) as e:
            discover.get_JSONL_iterators(
                options=options,
                iterator=records
            )
        self.assertEqual(str(e.exception), expected_data)
