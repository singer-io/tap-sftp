from base import TestSFTPBase
import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner
import os
import json
import zipfile


RECORD_COUNT = {}
class TestSFTPZip(TestSFTPBase):

    def name(self):
        return "tap_tester_sftp_zip"

    def get_files(self):
        return [
            {
                "headers": ['id', 'string_col', 'integer_col'],
                "directory": "folderA",
                "files": ["table_1_fileA.csv", "table_3_fileA.csv"],
                "archive": 'table_1.zip',
                "num_rows": 50,
                "generator": self.generate_simple_csv_lines_typeA
            },
            {
                "headers": ['id', 'string_col', 'datetime_col', 'number_col'],
                "directory": "folderB",
                "files": ["table_2_fileA.csv", "table_2_fileB.csv", "table_3_fileB.csv"],
                "archive": 'table_2.zip',
                "num_rows": 50,
                "generator": self.generate_simple_csv_lines_typeB
            },
            {
                "headers": ['id', 'string_col', 'integer_col'],
                "directory": "folderC",
                "files": ["table_3_fileC.csv"],
                "archive": 'table_3.zip',
                "num_rows": 50,
                "generator": self.generate_simple_csv_lines_typeA
            },
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
                TestSFTPZip.rm('tap_tester', client)
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
                client.chdir(directory)
                with client.open(file_group['archive'], 'w') as direct_file:
                    with zipfile.ZipFile(direct_file, mode='w') as zip_file:
                        lines = [headers] + file_group['generator'](file_group['num_rows'])
                        total = ''
                        for line in lines:
                            total += ','.join((str(val) for val in line)) + '\n'
                        for file_name in file_group['files']:
                            zip_file.writestr(file_name, total)
                client.chdir('..')

    def expected_first_sync_row_counts(self):
        return {
            'table_1': 100,
            'table_2': 150,
            'table_3': 50
        }

    def get_properties(self):
        props = self.get_common_properties()
        props['tables'] = json.dumps([
                {
                    "table_name": "table_1",
                    "search_prefix": os.getenv("TAP_SFTP_ROOT_DIR") + "/tap_tester",
                    "delimiter": ",",
                    "search_pattern": "table_1\.zip",
                    "key_properties": ['id']
                },
                {
                    "table_name": "table_2",
                    "search_prefix": os.getenv("TAP_SFTP_ROOT_DIR") + "/tap_tester",
                    "delimiter": ",",
                    "search_pattern": "table_2\.zip",
                    "key_properties": ['id'],
                    "date_overrides": ["datetime_col"]
                },
                {
                    "table_name": "table_3",
                    "search_prefix": os.getenv("TAP_SFTP_ROOT_DIR") + "/tap_tester",
                    "delimiter": ",",
                    "search_pattern": "table_3\.zip",
                    "key_properties": ['id'],
                    "date_overrides": ["datetime_col"]
                }
            ])
        return props

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
            with self.subTest(stream=tap_stream_id):
                found_stream = [c for c in catalog if c['tap_stream_id'] == tap_stream_id][0]

                schema_and_metadata = menagerie.get_annotated_schema(conn_id, found_stream['stream_id'])
                main_metadata = schema_and_metadata["metadata"]
                stream_metadata = [mdata for mdata in main_metadata if mdata["breadcrumb"] == []][0]

                # table-key-properties metadata
                self.assertEqual(self.expected_pks()[tap_stream_id],
                                 set(stream_metadata["metadata"]["table-key-properties"]))


        for stream_catalog in catalog:
            annotated_schema = menagerie.get_annotated_schema(conn_id, stream_catalog['stream_id'])
            selected_metadata = connections.select_catalog_and_fields_via_metadata(conn_id,
                                                                                   stream_catalog,
                                                                                   annotated_schema,
                                                                                   [])

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
