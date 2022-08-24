import unittest
from unittest import mock
from parameterized import parameterized
from tap_sftp import discover

class JSONLIterator:
    def __init__(self, data):
        self.data = data

    def decode(self, encoding):
        return self.data

class Iterable:
    def __init__(self, data, name=None):
        self.data = data
        self.name = name

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

class TestRowIterators(unittest.TestCase):
    """Test cases to verify we call 'row_iterator' for JSONL or CSV as per the file extension"""
    @parameterized.expand([
        ["csv", ["test.csv", [Iterable("")]], [1, 0]],
        ["jsonl", ["test.jsonl", [Iterable('{"id": 1}')]], [0, 1]],
        ["zip_csv", ["test.zip", [Iterable("", "test.csv")]], [1, 0]],
        ["zip_jsonl", ["test.zip", [Iterable("", "test.jsonl")]], [0, 1]],
        ["gz_csv", ["test.gz", [[Iterable(""), "test.csv"]]], [1, 0]],
        ["gz_jsonl", ["test.gz", [[Iterable(""), "test.jsonl"]]], [0, 1]],
    ])
    @mock.patch("tap_sftp.discover.get_JSONL_iterators")
    @mock.patch("singer_encodings.csv.get_row_iterator")
    @mock.patch("singer_encodings.compression.infer")
    def test_get_row_iterators_local(self, name, test_data, expected_data, mocked_infer, mocked_get_csv_row_iterator, mocked_get_JSONL_iterators):
        # Mock file iterable
        mocked_infer.return_value = test_data[1]
        options = {
            "file_name": test_data[0]
        }
        # Function call
        list(discover.get_row_iterators_local(iterable=[], options=options, infer_compression=True))
        # Verify the call count for JSONL or CSV row_iterator
        self.assertEqual(mocked_get_csv_row_iterator.call_count, expected_data[0])
        self.assertEqual(mocked_get_JSONL_iterators.call_count, expected_data[1])
