import logging
from base import TestSFTPBase
import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner
from functools import reduce
import os
import csv
import json

class TestSFTPBookmark(TestSFTPBase):

    def name(self):
        return "tap_tester_sftp_bookmark"

    def get_files(self):
        """Generate files for the test"""
        return [
            {
                "headers": ['id', 'string_col', 'integer_col'],
                "directory": "table_1_files",
                "files": ["table_1_fileA.csv", "table_3_fileA.csv"],
                "num_rows": 50,
                "generator": self.generate_simple_csv_lines_typeA
            },
            {
                "headers": ['id', 'string_col', 'datetime_col', 'number_col'],
                "directory": "table_2_files",
                "files": ["table_2_fileA.csv", "table_2_fileB.csv", "table_3_fileB.csv"],
                "num_rows": 50,
                "generator": self.generate_simple_csv_lines_typeB
            },
            {
                "headers": ['id', 'string_col', 'integer_col', 'datetime_col', 'number_col'],
                "directory": "table_3_files",
                "files": ["table_3_fileC.csv"],
                "num_rows": 50,
                "generator": self.generate_simple_csv_lines_typeC
            },
        ]

    def setUp(self):
        """Setup the directory for test """
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
                TestSFTPBookmark.rm('tap_tester', client)
            except FileNotFoundError:
                pass
            client.mkdir('tap_tester')

            # Add subdirectories
            client.mkdir('tap_tester/table_1_files')
            client.mkdir('tap_tester/table_2_files')
            client.mkdir('tap_tester/table_3_files')

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
        """Get table properties"""
        props = self.get_common_properties()
        props['tables'] = json.dumps([
                {
                    "table_name": "table_1",
                    "delimiter": ",",
                    "search_prefix": os.getenv("TAP_SFTP_ROOT_DIR") + "/tap_tester",
                    "search_pattern": "table_1.*csv",
                    "key_properties": ['id']
                },
                {
                    "table_name": "table_2",
                    "delimiter": ",",
                    "search_prefix": os.getenv("TAP_SFTP_ROOT_DIR") + "/tap_tester",
                    "search_pattern": "table_2.*csv",
                    "key_properties": ['id'],
                    "date_overrides": ["datetime_col"]
                },
                {
                    "table_name": "table_3",
                    "delimiter": ",",
                    "search_prefix": os.getenv("TAP_SFTP_ROOT_DIR") + "/tap_tester",
                    "search_pattern": "table_3.*csv",
                    "key_properties": ['id'],
                    "date_overrides": ["datetime_col"]
                }
            ])
        return props
    
    def expected_sync_streams(self):
        """Expected sync streams"""
        return {
            'table_1',
            'table_2',
            'table_3',
        }
    
    def test_run(self):
        """
        - Verify actual rows were synced.
        - Verify new records are synced after a sync job.
        """
        conn_id = connections.ensure_connection(self)
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Select our catalogs
        our_catalogs = [c for c in found_catalogs if c.get('tap_stream_id') in self.expected_sync_streams()]

        self.perform_and_verify_table_and_field_selection(conn_id, our_catalogs)

        # Clear state before our run
        menagerie.set_state(conn_id, {})

        # Run a sync job using orchestrator
        self.run_and_verify_sync(conn_id)

        # Verify actual rows were synced
        record_count_by_stream = runner.examine_target_output_file(self, conn_id, self.expected_sync_streams(), self.expected_pks())
        replicated_row_count =  reduce(lambda accum,c : accum + c, record_count_by_stream.values())
        self.assertGreater(replicated_row_count, 0, msg="failed to replicate any data: {}".format(record_count_by_stream))
        logging.info("total replicated row count: {}".format(replicated_row_count))

        # Creating file "table_1_fileB"
        with self.get_test_connection() as client:
            root_dir = os.getenv('TAP_SFTP_ROOT_DIR')
            client.chdir(root_dir + '/tap_tester/table_1_files')

            file_group = self.get_files()[0]
            with client.open('table_1_fileB.csv', 'w') as f:
                writer = csv.writer(f)
                lines = [file_group['headers']] + file_group['generator'](file_group['num_rows'])
                writer.writerows(lines)
        
        # Run another Sync
        sync_job_name = runner.run_sync_mode(self, conn_id)
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # Check that we synced new records.
        records = runner.get_records_from_target_output()
        messages = records.get('table_1').get('messages')
        self.assertEqual(len(messages), 50, msg="Sync'd incorrect count of messages: {}".format(len(messages)))
    
        # Run a final sync
        sync_job_name = runner.run_sync_mode(self, conn_id)
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # Check that we synced new records.
        records = runner.get_records_from_target_output()
        messages = records.get('table_1', {}).get('messages', [])
        self.assertEqual(len(messages), 0, msg="Sync'd incorrect count of messages: {}".format(len(messages)))


