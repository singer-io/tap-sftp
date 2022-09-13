from base import TestSFTPBase
import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner
import os
import csv
import json
from datetime import datetime as dt

RECORD_COUNT = {}

class TestSFTPStartDateOneStream(TestSFTPBase):
    """Test case to verify the Tap respected the start date provided in the config"""

    def get_files(self):
        return [
            {
                "headers": ['id', 'string_col', 'integer_col'],
                "directory": "test_start_date",
                "files": ["table.csv"],
                "num_rows": 50,
                "generator": self.generate_simple_csv_lines_typeA
            }
        ]

    def setUp(self):
        """Setup the directory for test """
        self.add_dir()

    def expected_first_sync_streams(self):
        return {
            'table'
        }

    def expected_check_streams(self):
        return {'table'}

    def expected_pks(self):
        return {
            'table': ['id']
        }

    def expected_columns(self):
        return {
            'table': ['id', 'integer_col', 'string_col']
        }

    def name(self):
        return "tap_tester_sftp_start_date"

    def get_properties(self, original: bool = True):
        props = self.get_common_properties()
        props['tables'] = json.dumps([
                {
                    "table_name": "table",
                    "delimiter": ",",
                    "search_prefix": os.getenv("TAP_SFTP_ROOT_DIR") + "/tap_tester",
                    "search_pattern": "table.*csv",
                    "key_properties": ['id']
                }
            ])
        if original:
            return props

        props["start_date"] = self.START_DATE
        return props

    def test_run(self):
        self.file_modified_test()
        self.file_not_modified_test()
    
    def file_modified_test(self):

        # Sync 1
        conn_id_1 = connections.ensure_connection(self)

        found_catalogs_1 = self.run_and_verify_check_mode(conn_id_1)

        self.perform_and_verify_table_and_field_selection(conn_id_1,found_catalogs_1)

        record_count_by_stream_1 = self.run_and_verify_sync(conn_id_1)
        synced_records_1 = runner.get_records_from_target_output()

        # Checking if we got any records
        self.assertGreater(sum(record_count_by_stream_1.values()), 0)

        # Changing start date to "utcnow"
        self.START_DATE = dt.strftime(dt.utcnow(), "%Y-%m-%dT00:00:00Z")

        # Adding some data to the file
        self.append_to_files()

        # Sync 2
        conn_id_2 = connections.ensure_connection(self, original_properties = False)

        found_catalogs_2 = self.run_and_verify_check_mode(conn_id_2)

        self.perform_and_verify_table_and_field_selection(conn_id_2,found_catalogs_2)

        record_count_by_stream_2 = self.run_and_verify_sync(conn_id_2)
        synced_records_2 = runner.get_records_from_target_output()

        # Checking if we got any data
        self.assertGreater(sum(record_count_by_stream_2.values()), 0)

        # Verifying if we got more data in sync 2 than sync 1
        self.assertGreater(sum(record_count_by_stream_2.values()), sum(record_count_by_stream_1.values()))

        for stream in self.expected_check_streams():
            expected_primary_keys = self.expected_pks()

            record_count_sync_1 = record_count_by_stream_1.get(stream, 0)
            record_count_sync_2 = record_count_by_stream_2.get(stream, 0)

            primary_keys_list_1 = [tuple(message.get('data').get(expected_pk) for expected_pk in expected_primary_keys)
                                    for message in synced_records_1.get(stream).get('messages')
                                    if message.get('action') == 'upsert']
            primary_keys_list_2 = [tuple(message.get('data').get(expected_pk) for expected_pk in expected_primary_keys)
                                    for message in synced_records_2.get(stream).get('messages')
                                    if message.get('action') == 'upsert']

            primary_keys_sync_1 = set(primary_keys_list_1)
            primary_keys_sync_2 = set(primary_keys_list_2)

            # Verify the number of records replicated in sync 2 is greater than the number
            # of records replicated in sync 1 for stream
            self.assertGreater(record_count_sync_2, record_count_sync_1)

            # Verify the records replicated in sync 1 were also replicated in sync 2
            self.assertTrue(primary_keys_sync_1.issubset(primary_keys_sync_2))

    def file_not_modified_test(self):

        # Sync 1
        conn_id_1 = connections.ensure_connection(self)

        found_catalogs_1 = self.run_and_verify_check_mode(conn_id_1)

        self.perform_and_verify_table_and_field_selection(conn_id_1,found_catalogs_1)

        record_count_by_stream1 = self.run_and_verify_sync(conn_id_1)

        self.assertGreater(sum(record_count_by_stream1.values()), 0)

        # Changing start date to "utcnow"
        self.START_DATE = dt.strftime(dt.utcnow(), "%Y-%m-%dT00:00:00Z")

        # Sync 2
        conn_id_2 = connections.ensure_connection(self, original_properties = False)

        found_catalogs_2 = self.run_and_verify_check_mode(conn_id_2)

        self.perform_and_verify_table_and_field_selection(conn_id_2,found_catalogs_2)

        # As we have not added any data, so file is not modified and
        # We should not get any data and recieve error: failed to replicate any data
        try:
            self.run_and_verify_sync(conn_id_2)
        except AssertionError as e:
            self.assertRegex(str(e), r'failed to replicate any data')

