from base import TestSFTPBase
import tap_tester.connections as connections
import os
import json
import zipfile

RECORD_COUNT = {}

class TestSFTPEmptyCSVInZIP(TestSFTPBase):

    def name(self):
        return "tap_tester_sftp_empty_csv_in_zip"

    def get_files(self):
        return [
            {
                # empty 'csv in zip' file as 'num_rows' is not given
                "headers": [],
                "files": ["table_1_empty.csv"],
                "archive": 'table_1.zip',
            },
            {
                "headers": ['id', 'string_col', 'integer_col'],
                "files": ["table_2_non_empty.csv", "table_2_empty.csv"],
                "archive": 'table_2.zip',
                "num_rows": 50,
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
                TestSFTPEmptyCSVInZIP.rm('tap_tester', client)
            except FileNotFoundError:
                pass
            client.mkdir('tap_tester')
            client.chdir('tap_tester')

            file_info = self.get_files()
            client.mkdir("test_empty_zip")

            # Add csv files
            for file_group in file_info:
                headers = file_group['headers']
                directory = "test_empty_zip"
                client.chdir(directory)
                with client.open(file_group['archive'], 'w') as direct_file:
                    with zipfile.ZipFile(direct_file, mode='w') as zip_file:
                        total = ''
                        # write in file if 'num_rows', used to create an empty 'csv' file
                        if file_group.get('num_rows'):
                            lines = [headers] + file_group['generator'](file_group['num_rows'])
                            for line in lines:
                                total += ','.join((str(val) for val in line)) + '\n'
                        for file_name in file_group['files']:
                            if file_name in ["table_2_empty.csv"]:
                                total = ''
                            zip_file.writestr(file_name, total)
                client.chdir('..')

    def expected_check_streams(self):
        return {
            'table'
        }

    def expected_pks(self):
        return {
            'table': {}
        }

    def expected_sync_row_counts(self):
        return {
            'table': 50
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
                    "search_prefix": os.getenv("TAP_SFTP_ROOT_DIR") + "/tap_tester",
                    "search_pattern": "table.*zip",
                    "key_properties": []
                }
            ])
        if original:
            return props

        props["start_date"] = self.START_DATE
        return props

    def test_run(self):
        # sync
        conn_id = connections.ensure_connection(self)

        found_catalogs = self.run_and_verify_check_mode(conn_id)

        self.perform_and_verify_table_and_field_selection(conn_id,found_catalogs)

        record_count_by_stream = self.run_and_verify_sync(conn_id)

        # checking if we got any data from sync
        self.assertGreater(sum(record_count_by_stream.values()), 0)

        # checking if data after sync is as expected
        for tap_stream_id in self.expected_first_sync_streams():
            self.assertEqual(self.expected_sync_row_counts()[tap_stream_id],
                             record_count_by_stream[tap_stream_id])
