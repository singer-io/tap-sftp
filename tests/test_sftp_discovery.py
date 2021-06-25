from base import TestSFTPBase
import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner
import os
import csv
import json

class TestSFTPDiscovery(TestSFTPBase):

    def name(self):
        return "tap_tester_sftp_discovery"

    def get_files(self):
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

    def test_run(self):
        conn_id = connections.ensure_connection(self)

        # run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # check exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        # tap discovered the right streams
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

                automatic_fields = [mdata["breadcrumb"][1]
                                    for mdata in main_metadata
                                    if mdata["metadata"]["inclusion"] == "automatic"]

                # table-key-properties metadata
                self.assertEqual(self.expected_pks()[tap_stream_id],
                                 set(stream_metadata["metadata"]["table-key-properties"]))

                # replication method check
                self.assertEqual('INCREMENTAL',
                                 stream_metadata["metadata"]['forced-replication-method'])

                # check if all columns are present or not
                self.assertTrue(
                    set(
                        self.expected_columns()[tap_stream_id]
                    ).issubset(
                        set(
                            schema_and_metadata['annotated-schema']['properties'].keys()
                        )
                    )
                )

                # check if only primary key is "automatic"
                self.assertEqual(self.expected_pks()[tap_stream_id],
                                 set(automatic_fields))

                # check all other fields are "available"
                self.assertTrue(
                        all({available_items["metadata"]["inclusion"] == "available"
                             for available_items in main_metadata
                             if available_items.get("breadcrumb", []) != []
                             and available_items.get("breadcrumb", ["properties", None])[1]
                             not in automatic_fields}),
                        msg="Not all non key properties are set to available in metadata")
