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

    def name(self):
        return "tap_tester_sftp_ordered_files"

    # get file files details to add data
    def get_files(self):
        return [
            {
                "headers": ['id', 'string_col', 'integer_col'],
                "directory": "folderA",
                "files": ["table_1_file.csv", "table_2_file.csv", "table_3_file.csv", "table_4_file.csv", "table_5_file.csv"],
                # adding greater number of rows as it takes some time
                # so we can test modified date
                "num_rows": 1500,
                "generator": self.generate_simple_csv_lines_typeA
            }
        ]

    def setUp(self):
        if not all([x for x in [os.getenv('TAP_SFTP_USERNAME'),
                                os.getenv('TAP_SFTP_PASSWORD'),
                                os.getenv('TAP_SFTP_ROOT_DIR')]]):
            #pylint: disable=line-too-long
            raise Exception("set TAP_SFTP_USERNAME, TAP_SFTP_PASSWORD, TAP_SFTP_ROOT_DIR")

        root_dir = os.getenv('TAP_SFTP_ROOT_DIR')

        with self.get_test_connection() as client:
            # drop all csv files in root dir
            client.chdir(root_dir)
            try:
                TestSFTPOrderedFiles.rm('tap_tester', client)
            except FileNotFoundError:
                pass
            client.mkdir('tap_tester')
            client.chdir('tap_tester')

            # Add subdirectories
            file_info = self.get_files()
            for entry in file_info:
                client.mkdir(entry['directory'])

            # Add csv files
            for file_group in file_info:
                headers = file_group['headers']
                directory = file_group['directory']
                for filename in file_group['files']:
                    client.chdir(directory)
                    with client.open(filename, 'w') as f:
                        writer = csv.writer(f)
                        lines = [headers] + file_group['generator'](file_group['num_rows'])
                        writer.writerows(lines)
                    client.chdir('..')

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

    # returns the last modified date of all the files present in the folder
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

        # append some data to particular files to test the modified date
        self.append_to_files(["table_1_file.csv", "table_3_file.csv", "table_4_file.csv"])

        # sync
        conn_id = connections.ensure_connection(self)

        found_catalogs = self.run_and_verify_check_mode(conn_id)

        self.perform_and_verify_table_and_field_selection(conn_id,found_catalogs)

        record_count_by_stream = self.run_and_verify_sync(conn_id)

        state = menagerie.get_state(conn_id)

        # checking if we got any data from sync
        self.assertGreater(sum(record_count_by_stream.values()), 0)

        # checking if data after sync is as expected
        for tap_stream_id in self.expected_first_sync_streams():
            self.assertEqual(self.expected_sync_row_counts()[tap_stream_id],
                             record_count_by_stream[tap_stream_id])

        # getting maximum of last mofified dates from all files
        max_date = max(self.get_last_modified()).replace(microsecond = 0)
        expected_date = max_date.timestamp()

        # getting bookmark
        actual_date = datetime.datetime.fromisoformat(state['bookmarks']['table']['modified_since']).timestamp()

        # checking if maximum last modified date is set as bookmark
        self.assertEqual(int(expected_date), int(actual_date))
