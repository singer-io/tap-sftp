from base import TestSFTPBase
from functools import reduce
import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner
import os
import csv
import json
import logging

class TestSFTPDatatype(TestSFTPBase):    
    def name(self):
            return "tap_tester_sftp_Datatype"

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
                TestSFTPDatatype.rm('tap_tester', client)
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
    def expected_check_streams(self):
        """Expected check streams"""
        return {
            'table_1',
            'table_2',
            'table_3',
        }
    def expected_pks(self):
        """Expected primary keys"""
        return {
            'table_1': {"id"},
            'table_2': {"id"},
            'table_3': {"id"},
        }

    def expected_sync_row_counts(self):
        """Expected row count"""
        return {
            'table_1': 100,
            'table_2': 150,
            'table_3': 150
        }

    def test_discovery_run(self):
        conn_id = connections.ensure_connection(self)
        # Run in discovery mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # Verify check exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        # Verify the tap discovered the right streams
        found_catalogs = menagerie.get_catalogs(conn_id)

        # Assert we find the correct streams
        self.assertEqual(self.expected_check_streams(),
                         {c['tap_stream_id'] for c in found_catalogs})
        
        for tap_stream_id in self.expected_check_streams():
            found_stream = [c for c in found_catalogs if c['tap_stream_id'] == tap_stream_id][0]

            # Assert that the pks are correct
            self.assertEqual(self.expected_pks()[found_stream['stream_name']],
                             set(found_stream.get('metadata', {})[0].get('metadata', {}).get('table-key-properties')))

    def test_run_sync_mode(self):
        conn_id = connections.ensure_connection(self)
        
        # Select our catalogs
        our_catalogs = self.run_and_verify_check_mode(conn_id)
        self.perform_and_verify_table_and_field_selection(conn_id, our_catalogs, True)

        # Clear state before our run
        menagerie.set_state(conn_id, {})

        # Run a sync job using orchestrator
        self.run_and_verify_sync(conn_id)

        # Verify actual rows were synced
        record_count_by_stream = runner.examine_target_output_file(
            self,
            conn_id,
            self.expected_check_streams(),
            self.expected_pks())
        replicated_row_count = reduce(lambda accum, c : accum + c, record_count_by_stream.values())

        for stream in self.expected_check_streams():
            with self.subTest(stream=stream):
                self.assertEqual(
                    record_count_by_stream.get(stream, 0),
                    self.expected_sync_row_counts()[stream],
                    msg="actual rows: {}, expected_rows: {} for stream {} don't match".format(
                        record_count_by_stream.get(stream, 0),
                        self.expected_sync_row_counts()[stream],
                        stream)
                )

        logging.info("total replicated row count: {}".format(replicated_row_count))
    
    def test_primary_keys(self):
        conn_id = connections.ensure_connection(self)
        found_catalogs = self.run_and_verify_check_mode(conn_id)
        all_catalogs = [x for x in found_catalogs]
        for catalog in all_catalogs:
            with self.subTest(c=catalog):
                expected_key_properties = \
                    self.expected_pks()[catalog["stream_name"]]
                metadata_and_annotated_schema = menagerie.get_annotated_schema(
                    conn_id, catalog['stream_id'])

                # Verify that expected_key_properties show as automatic in metadata
                metadata = metadata_and_annotated_schema["metadata"]
                actual_key_properties = {item.get("breadcrumb", ["", ""])[1]
                                         for item in metadata
                                         if item.get("metadata").get("inclusion") == "automatic"}
                self.assertEqual(actual_key_properties, expected_key_properties)