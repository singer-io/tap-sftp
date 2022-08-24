import unittest
from parameterized import parameterized
from unittest import mock
from tap_sftp import client, sync
import singer

class TestSyncJSONLsdcFields(unittest.TestCase):
    @parameterized.expand([
        ["with_sdc_fields", [[{"id": 1}]], {'id': 1, '_sdc_source_file': '/root_dir/data.jsonl', '_sdc_source_lineno': 2}],
        ["with_sdc_extra", [[{"id": 1, "name": "abc"}]], {'id': 1, 'name': 'abc', '_sdc_source_file': '/root_dir/data.jsonl', '_sdc_source_lineno': 2, '_sdc_extra': [{"name": "abc"}]}]
    ])
    @mock.patch("tap_sftp.client.SFTPConnection.sftp")
    @mock.patch('singer.Transformer.transform')
    @mock.patch('singer_encodings.csv.get_row_iterators')
    def test_sync_JSONL(self, name, test_data, expected_data, mocked_get_row_iterators, mocked_transform, mocked_sftp):
        mocked_get_row_iterators.return_value = test_data
        conn = client.SFTPConnection("10.0.0.1", "username", port="22")
        table_spec = {
            "key_properties": [],
            "delimiter": None,
            "table_name": "test",
            "search_prefix": None,
            "search_pattern": "data.jsonl"
        }
        stream = singer.CatalogEntry(tap_stream_id="test", \
            schema=singer.Schema(properties={"id": singer.Schema(type=["null", "integer"])}), metadata=[])
        f = {"filepath": "/root_dir/data.jsonl", "last_modified": "2022-01-01"}
        sync.sync_file(conn=conn, f=f, stream=stream, table_spec=table_spec)
        args = mocked_transform.call_args.args
        records = args[0]
        self.assertEqual(records, expected_data)
