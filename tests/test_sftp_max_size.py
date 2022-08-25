from base import TestSFTPBase
from tap_tester import connections, runner
import os
import csv
import json

class TestSFTPDiscovery(TestSFTPBase):

    def name(self):
        return "tap_tester_sftp_maximize_csv_field_width"

    def get_files(self):
        return [
            {
                "headers": ['id', 'name'],
                "directory": "max_csv",
                "files": ["max_csv_file.csv"],
                "num_rows": 1,
                "generator": self.generate_max_size_csv
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
                TestSFTPDiscovery.rm('tap_tester', client)
            except FileNotFoundError:
                pass
            client.mkdir('tap_tester')

            # Add subdirectories
            client.mkdir('tap_tester/max_csv')

            # Add csv files
            client.chdir('tap_tester')

            for file_group in self.get_files():
                headers = file_group['headers']
                directory = file_group['directory']
                for filename in file_group['files']:
                    client.chdir(directory)
                    with client.open(filename, 'w') as f:
                        writer = csv.writer(f)
                        lines = [headers] + file_group['generator']()
                        writer.writerows(lines)
                    client.chdir('..')

    def get_properties(self):
        props = self.get_common_properties()
        props['tables'] = json.dumps([
                {
                    "table_name": "csv_with_max_field_width",
                    "delimiter": ",",
                    "search_prefix": os.getenv("TAP_SFTP_ROOT_DIR") + "/tap_tester",
                    "search_pattern": "max_csv_file.csv",
                    "key_properties": ['id']
                }
            ])
        return props

    def expected_first_sync_streams(self):
        return {'csv_with_max_field_width'}

    def expected_check_streams(self):
        return {'csv_with_max_field_width'}

    def expected_pks(self):
        return {
            'csv_with_max_field_width': {'id'}
        }

    def test_run(self):
        conn_id = connections.ensure_connection(self)

        found_catalogs = self.run_and_verify_check_mode(conn_id)

        self.perform_and_verify_table_and_field_selection(conn_id, found_catalogs)

        self.run_and_verify_sync(conn_id)

        expected_records = 1
        record_count = runner.get_upserts_from_target_output()
        # Verify record counts
        self.assertEqual(expected_records, len(record_count))

        records = runner.get_records_from_target_output()
        actual_records = [record.get('data') for record in records.get('csv_with_max_field_width').get('messages')]
        # Verify the record we created of length greater than 'csv.field_size_limit' of '131072' is replicated
        self.assertEqual(actual_records, [{'id': 1, 'name': '{}'.format('a'*131074), '_sdc_source_file': os.getenv('TAP_SFTP_ROOT_DIR') + '/tap_tester/max_csv/max_csv_file.csv', '_sdc_source_lineno': 2}])
