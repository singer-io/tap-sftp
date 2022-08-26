from base import TestSFTPBase
import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner
import os
import csv
import json

class TestSFTPAutomatic(TestSFTPBase):

    def name(self):
        return "tap_tester_sftp_automatic_fields"

    def get_files(self):
        return [
            {
                "headers": ['id', 'string_col', 'integer_col'],
                "directory": "table_1_files",
                "files": ["table_1_fileA.csv"],
                "num_rows": 50,
                "generator": self.generate_simple_csv_lines_typeA
            }
        ]

    def setUp(self):
        if not all([x for x in [os.getenv('TAP_SFTP_USERNAME'),
                                os.getenv('TAP_SFTP_PASSWORD'),
                                os.getenv('TAP_SFTP_ROOT_DIR')]]):
            # pylint: disable=line-too-long
            raise Exception("set TAP_SFTP_USERNAME, TAP_SFTP_PASSWORD, TAP_SFTP_ROOT_DIR")

        root_dir = os.getenv('TAP_SFTP_ROOT_DIR')

        with self.get_test_connection() as client:
            # Drop all csv files in root dir
            client.chdir(root_dir)
            try:
                TestSFTPAutomatic.rm('tap_tester', client)
            except FileNotFoundError:
                pass
            client.mkdir('tap_tester')

            # Add subdirectories
            client.mkdir('tap_tester/table_1_files')

            # Add csv files
            client.chdir('tap_tester')

            for file_group in self.get_files():
                headers = file_group['headers']
                directory = file_group['directory']
                for filename in file_group['files']:
                    client.chdir(directory)
                    with client.open(filename, 'w') as f:
                        writer = csv.writer(f)
                        lines = [headers] + file_group['generator'](file_group['num_rows'])
                        writer.writerows(lines)
                    client.chdir('..')

    def get_properties(self):
        props = self.get_common_properties()
        props['tables'] = json.dumps([
                {
                    "table_name": "table_1",
                    "delimiter": ",",
                    "search_prefix": os.getenv("TAP_SFTP_ROOT_DIR") + "/tap_tester",
                    "search_pattern": "table_1.*csv",
                    "key_properties": ['id']
                }
            ])
        return props

    def expected_sync_streams(self):
        return {
            'table_1',
        }
    def expected_check_streams(self):
        return {
            'table_1',
        }

    def expected_pks(self):
        return {
            'table_1': {'id'},
        }

    def expected_automatic_fields(self):
        return {
            'table_1': {'id'},
        }

    def test_run(self):

        conn_id = connections.ensure_connection(self)
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Clear state before our run
        menagerie.set_state(conn_id, {})
        
        expected_streams =  self.expected_sync_streams()
        test_catalogs_automatic_fields = [catalog for catalog in found_catalogs
                                          if catalog.get('stream_name') in expected_streams]

        self.perform_and_verify_table_and_field_selection(
            conn_id, test_catalogs_automatic_fields, select_all_fields=False,
        )

        # Run a sync job using orchestrator
        self.run_and_verify_sync(conn_id)

        synced_records = runner.get_records_from_target_output()

        for stream in self.expected_sync_streams():
            with self.subTest(stream=stream):

                expected_keys = self.expected_automatic_fields().get(stream)

                # Collect actual values
                data = synced_records.get(stream, {})
                record_messages_keys = [set(row.get('data').keys()) for row in data.get('messages', {})]

                for actual_keys in record_messages_keys:
                    self.assertSetEqual(expected_keys, actual_keys)
