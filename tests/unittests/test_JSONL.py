import unittest
from unittest import mock
from parameterized import parameterized
from tap_sftp import discover
from singer_encodings.jsonl import get_JSONL_iterators

class JSONLIterator:
    def __init__(self, data):
        self.data = data

    def decode(self, encoding):
        return self.data

class TestRowIterators(unittest.TestCase):
    """Test cases to verify we call 'row_iterator' for JSONL or CSV as per the file extension"""
    @parameterized.expand([
        ["csv", ["test.csv", [[b'id,name\n', b'1,test1\n']]], 1],
        ["jsonl", ["test.jsonl", [[b'{"id": 1, "name": "test1"}\n']]], 0],
        ["zip_csv", ["test.zip", [[b'id,name\n', b'1,test1\n']]], 1],
        ["zip_jsonl", ["test.zip", [[b'{"id": 1, "name": "test1"}\n']]], 0],
        ["gz_csv", ["test.gz", [[b'id,name\n', b'1,test1\n']]], 1],
        ["gz_jsonl", ["test.gz", [[b'{"id": 1, "name": "test1"}\n']]], 0],
    ])
    @mock.patch("singer_encodings.jsonl.get_JSONL_iterators", side_effect=get_JSONL_iterators)
    @mock.patch("singer_encodings.csv.get_row_iterator")
    @mock.patch("tap_sftp.discover.compression_infer_local")
    def test_get_row_iterators_local(self, name, test_data, expected_data, mocked_infer, mocked_get_csv_row_iterator, mocked_get_JSONL_iterators):
        # Mock file iterable
        mocked_infer.return_value = test_data[1]
        options = {
            "file_name": test_data[0]
        }
        # Function call
        list(discover.get_row_iterators_local(iterable=[], options=options, infer_compression=True))
        # Verify the call count for JSONL or CSV row_iterator
        self.assertEqual(mocked_get_csv_row_iterator.call_count, expected_data)
        # Verify we try to parse for JSONL first for every file
        self.assertEqual(mocked_get_JSONL_iterators.call_count, 1)
