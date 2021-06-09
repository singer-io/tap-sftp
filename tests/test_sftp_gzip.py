from base import TestSFTPBase
from tap_tester.scenario import (SCENARIOS)
import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner
import os
import csv
import json
import gzip
import io

RECORD_COUNT = {}

class TestSFTPGzip(TestSFTPBase):

    def name(self):
        return "tap_tester_sftp_gzip"

    def get_files(self):
        return [
            {
                "headers": ['id', 'string_col', 'integer_col'],
                "directory": "folderA",
                "files": ["table_1_fileA.csv.gz", "table_3_fileA.csv.gz"],
                "num_rows": 50,
                "generator": self.generate_simple_csv_lines_typeA
            },
            {
                "headers": ['id', 'string_col', 'datetime_col', 'number_col'],
                "directory": "folderB",
                "files": ["table_2_fileA.csv.gz", "table_2_fileB.csv.gz", "table_3_fileB.csv.gz"],
                "num_rows": 50,
                "generator": self.generate_simple_csv_lines_typeB
            },
            {
                "headers": ['id', 'string_col', 'integer_col', 'datetime_col', 'number_col'],
                "directory": "folderC",
                "files": ["table_3_fileC.csv.gz"],
                "num_rows": 50,
                "generator": self.generate_simple_csv_lines_typeC
            },
        ]

    def setUp(self):
        if not all([x for x in [os.getenv('TAP_SFTP_USERNAME'),
                                os.getenv('TAP_SFTP_PASSWORD'),
                                os.getenv('TAP_SFTP_ROOT_DIR'),
                                os.getenv('TAP_SFTP_PRIVATE_KEY_FILE')]]):
            #pylint: disable=line-too-long
            raise Exception("set TAP_SFTP_USERNAME, TAP_SFTP_PASSWORD, TAP_SFTP_ROOT_DIR, TAP_SFTP_PRIVATE_KEY_FILE")

        root_dir = os.getenv('TAP_SFTP_ROOT_DIR')

        with self.get_test_connection() as client:
            # drop all csv files in root dir
            client.chdir(root_dir)
            try:
                TestSFTPGzip.rm('tap_tester', client)
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
                    with client.open(filename, 'w') as direct_file:
                        with gzip.GzipFile(fileobj=direct_file, mode='w') as gzip_file:
                            with io.TextIOWrapper(gzip_file, encoding='utf-8') as f:
                                writer = csv.writer(f)
                                lines = [headers] + file_group['generator'](file_group['num_rows'])
                                writer.writerows(lines)
                    client.chdir('..')

    def get_properties(self):
        return {
            'start_date' : '2017-01-01 00:00:00',
            'host' : os.getenv('TAP_SFTP_HOST'),
            'port' : os.getenv('TAP_SFTP_PORT'),
            'username' : os.getenv('TAP_SFTP_USERNAME'),
            'private_key_file': None if os.getenv('TAP_SFTP_PRIVATE_KEY_FILE') == "None" else os.getenv('TAP_SFTP_PRIVATE_KEY_FILE'),
            'tables': json.dumps([
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
        }

    def test_run(self):

        conn_id = connections.ensure_connection(self)

        # run in discovery mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # verify check  exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        # verify the tap discovered the right streams
        catalog = menagerie.get_catalogs(conn_id)
        found_catalog_names = set(map(lambda c: c['tap_stream_id'], catalog))

        # assert we find the correct streams
        self.assertEqual(self.expected_check_streams(),
                         found_catalog_names)


        for tap_stream_id in self.expected_check_streams():
            found_stream = [c for c in catalog if c['tap_stream_id'] == tap_stream_id][0]
            stream_metadata = found_stream['metadata']
            main_metadata = [mdata for mdata in stream_metadata if mdata["breadcrumb"] == []]

            # assert that the pks are correct
            self.assertEqual(self.expected_pks()[found_stream['stream']],
                             set(main_metadata[0]["metadata"]["table-key-properties"]))

        for stream_catalog in catalog:
            annotated_schema = menagerie.get_annotated_schema(conn_id, stream_catalog['stream_id'])
            additional_md = [{ "breadcrumb" : [], "metadata" : {'replication-method' : 'INCREMENTAL'}}]
            selected_metadata = connections.select_catalog_and_fields_via_metadata(conn_id,
                                                                                   stream_catalog,
                                                                                   annotated_schema,
                                                                                   additional_md)

        # Run sync
        sync_job_name = runner.run_sync_mode(self, conn_id)

        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # verify the persisted schema was correct
        messages_by_stream = runner.get_records_from_target_output()

        # assert that each of the streams that we synced are the ones that we expect to see
        record_count_by_stream = runner.examine_target_output_file(self,
                                                                   conn_id,
                                                                   self.expected_first_sync_streams(),
                                                                   self.expected_pks())

        # Verify that the full table was syncd
        for tap_stream_id in self.expected_first_sync_streams():
            self.assertEqual(self.expected_first_sync_row_counts()[tap_stream_id],
                             record_count_by_stream[tap_stream_id])

SCENARIOS.add(TestSFTPGzip)
