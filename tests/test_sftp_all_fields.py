from base import TestSFTPBase
import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner
import os
import csv
import json

class TestSFTPAllFields(TestSFTPBase):

    def name(self):
        return "tap_tester_sftp_all_fields"

    def generate_simple_csv_lines_typeA(self, num_lines):
        """Overriding the function to generate '_sdc_extra_' field"""
        lines = []
        for int_value in range(num_lines):
            lines.append([int_value, self.random_string_generator(), int_value*5, 'extra_field_1', 'extra_field_2' ])
        return lines

    def get_files(self):
        """Generate files for the test"""
        return [
            {
                "headers": ['id', 'string_col', 'string_col'],
                "directory": "table_1_files",
                "files": ["table_1_fileA.csv"],
                "num_rows": 50,
                "generator": self.generate_simple_csv_lines_typeA
            },
        ]

    def setUp(self):
        """Setup the directory for test """
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
                TestSFTPAllFields.rm('tap_tester', client)
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
        """Get table properties"""
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

    def expected_check_streams(self):
        """Expected streams"""
        return {
            'table_1',
        }

    def expected_pks(self):
        """Expected primary keys"""
        return {
            'table_1': {'id'},
        }

    def test_run(self):
        """
        Ensure running the tap with all streams and fields selected results in the
        Replication of all fields.
        - Verify no unexpected streams were replicated
        """
        conn_id = connections.ensure_connection(self)
        expected_streams = self.expected_check_streams()
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Select our catalogs
        our_catalogs = [c for c in found_catalogs if c.get('tap_stream_id') in expected_streams]

        self.perform_and_verify_table_and_field_selection(conn_id, our_catalogs, True)

        stream_to_all_catalog_fields = dict()
        for c in our_catalogs:
            c_annotated = menagerie.get_annotated_schema(conn_id, c['stream_id'])
            connections.select_catalog_and_fields_via_metadata(conn_id, c, c_annotated, [], [])
            fields_from_field_level_md = [md_entry['breadcrumb'][1]
                                          for md_entry in c_annotated['metadata']
                                          if md_entry['breadcrumb'] != []]
            stream_to_all_catalog_fields[c['stream_name']] = set(fields_from_field_level_md)

        # Clear state before our run
        menagerie.set_state(conn_id, {})

        self.run_and_verify_sync(conn_id)
        self.perform_and_verify_table_and_field_selection(conn_id,found_catalogs)

        synced_records = runner.get_records_from_target_output()


        # Verify no unexpected streams were replicated
        synced_stream_names = set(synced_records.keys())
        self.assertSetEqual(expected_streams, synced_stream_names)

        for stream in self.expected_check_streams():
                with self.subTest(stream=stream):

                    # Expected values
                    expected_all_keys = stream_to_all_catalog_fields[stream]

                    messages = synced_records.get(stream)
                    # Collect actual values
                    actual_all_keys = set()
                    for message in messages['messages']:
                        if message['action'] == 'upsert':
                            actual_all_keys.update(message['data'].keys())

                    self.assertSetEqual(expected_all_keys, actual_all_keys)
