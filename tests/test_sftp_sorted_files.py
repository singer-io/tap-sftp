import time
from base import TestSFTPBase
import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner
import os
import csv
import json
import datetime

RECORD_COUNT = {}

class TestSFTPOrderedFiles(TestSFTPBase):
    """Test case to verify tap sorts the files in expected manner"""

    def name(self):
        return "tap_tester_sftp_ordered_files"

    # Get file files details to add data
    def get_files(self):
        return [
            {
                "headers": ['id', 'string_col', 'integer_col'],
                "directory": "folderA",
                "files": ["table_1_file.csv", "table_2_file.csv", "table_3_file.csv", "table_4_file.csv", "table_5_file.csv"],
                # Adding greater number of rows as it takes some time so we can test modified date
                "num_rows": 1500,
                "generator": self.generate_simple_csv_lines_typeA
            }
        ]

    def setUp(self):
        """Setup the directory for test """
        self.add_dir()

    def expected_check_streams(self):
        return {
            'table'
        }

    def expected_pks(self):
        return {
            'table': {'id'}
        }

    def expected_sync_row_counts(self):
        return {
            'table': 12000
        }

    def expected_first_sync_streams(self):
        return {
            'table'
        }

    def get_properties(self, original: bool = True):
        props = self.get_common_properties()
        props['tables'] = json.dumps([
                {
                    "table_name": "table",
                    "delimiter": ",",
                    "search_prefix":  os.getenv("TAP_SFTP_ROOT_DIR") + "/tap_tester",
                    "search_pattern": "table.*csv",
                    "key_properties": ['id']
                }
            ])
        if original:
            return props
        
        props["start_date"] = self.START_DATE
        return props

    # Returns the last modified date of all the files present in the folder
    def get_last_modified(self):
        root_dir = os.getenv('TAP_SFTP_ROOT_DIR')

        with self.get_test_connection() as client:
            client.chdir(root_dir + '/tap_tester/folderA')
            files = client.listdir_attr('.')
            last_modified = []
            for file in files:
                last_modified.append(datetime.datetime.fromtimestamp(file.st_mtime))

        return last_modified

    def test_run(self):

        # Append some data to particular files to test the modified date
        self.append_to_files(["table_1_file.csv", "table_3_file.csv", "table_4_file.csv"])

        # Sync
        conn_id = connections.ensure_connection(self)

        found_catalogs = self.run_and_verify_check_mode(conn_id)

        self.perform_and_verify_table_and_field_selection(conn_id,found_catalogs)

        record_count_by_stream = self.run_and_verify_sync(conn_id)

        state = menagerie.get_state(conn_id)

        # Checking if we got any data from sync
        self.assertGreater(sum(record_count_by_stream.values()), 0)

        # Checking if data after sync is as expected
        for tap_stream_id in self.expected_first_sync_streams():
            self.assertEqual(self.expected_sync_row_counts()[tap_stream_id],
                             record_count_by_stream[tap_stream_id])

        # Getting maximum of last mofified dates from all files
        max_date = max(self.get_last_modified()).replace(microsecond = 0)
        expected_date = max_date.timestamp()

        # Getting bookmark
        actual_date = datetime.datetime.fromisoformat(state['bookmarks']['table']['modified_since']).timestamp()

        # Checking if maximum last modified date is set as bookmark
        self.assertEqual(int(expected_date), int(actual_date))
