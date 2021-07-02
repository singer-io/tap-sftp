from base import TestSFTPBase
import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner
import os
import csv
import json

RECORD_COUNT = {}

class TestSFTPStartDateMultipleStream(TestSFTPBase):

    def name(self):
        return "tap_tester_sftp_start_date_multiple_stream"

    def get_files(self):
        return [
            {
                "headers": ['id', 'string_col', 'integer_col'],
                "directory": "folderA",
                "files": ["table_1_fileA.csv", "table_3_fileA.csv"],
                "num_rows": 50,
                "generator": self.generate_simple_csv_lines_typeA
            },
            {
                "headers": ['id', 'string_col', 'datetime_col', 'number_col'],
                "directory": "folderB",
                "files": ["table_2_fileA.csv", "table_2_fileB.csv", "table_3_fileB.csv"],
                "num_rows": 50,
                "generator": self.generate_simple_csv_lines_typeB
            },
            {
                "headers": ['id', 'string_col', 'integer_col', 'datetime_col', 'number_col'],
                "directory": "folderC",
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
                TestSFTPStartDateMultipleStream.rm('tap_tester', client)
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

    def get_properties(self, original: bool = True):
        props = self.get_common_properties()
        props['tables'] = json.dumps([
                {
                    "table_name": "table_1",
                    "delimiter": ",",
                    "search_prefix":  os.getenv("TAP_SFTP_ROOT_DIR") + "/tap_tester",
                    "search_pattern": "table_1.*csv",
                    "key_properties": ['id']
                },
                {
                    "table_name": "table_2",
                    "search_prefix":  os.getenv("TAP_SFTP_ROOT_DIR") + "/tap_tester",
                    "search_pattern": "table_2.*csv",
                    "delimiter": ",",
                    "key_properties": ['id'],
                    "date_overrides": ["datetime_col"]
                },
                {
                    "table_name": "table_3",
                    "search_prefix":  os.getenv("TAP_SFTP_ROOT_DIR") + "/tap_tester",
                    "search_pattern": "table_3.*csv",
                    "key_properties": ['id'],
                    "delimiter": ",",
                    "date_overrides": ["datetime_col"]
                }
            ])
        if original:
            return props
        
        props["start_date"] = self.START_DATE
        return props

    def test_run(self):

        # sync 1
        conn_id = connections.ensure_connection(self)

        found_catalogs_1 = self.run_and_verify_check_mode(conn_id)

        self.perform_and_verify_table_and_field_selection(conn_id,found_catalogs_1)

        record_count_by_stream_1 = self.run_and_verify_sync(conn_id)

        # checking if we got any data from sync 1
        self.assertGreater(sum(record_count_by_stream_1.values()), 0)

        for tap_stream_id in self.expected_first_sync_streams():
            self.assertEqual(self.expected_first_sync_row_counts()[tap_stream_id],
                             record_count_by_stream_1[tap_stream_id])

        # getting state
        state = menagerie.get_state(conn_id)

        # creating file "table_1_fileB"
        with self.get_test_connection() as client:
            root_dir = os.getenv('TAP_SFTP_ROOT_DIR')
            client.chdir(root_dir + '/tap_tester/folderA')

            file_group = self.get_files()[0]
            with client.open('table_1_fileB.csv', 'w') as f:
                writer = csv.writer(f)
                lines = [file_group['headers']] + file_group['generator'](file_group['num_rows'])
                writer.writerows(lines)

        # adding some data to file "table_1_fileA" and "table_3_fileA"
        self.append_to_files()

        # setting state
        menagerie.set_state(conn_id, state)

        # sync 2
        record_count_by_stream_2 = self.run_and_verify_sync(conn_id, second_sync = True)

        # checking if we got any data from sync 2
        self.assertGreater(sum(record_count_by_stream_2.values()), 0)

        # checking if data after in 2nd sync is as expected
        # here as we have modified start date, so we should recieve only modified data
        # ie. after appending and creating file
        for tap_stream_id in self.expected_second_sync_streams():
            self.assertEqual(self.expected_second_sync_row_counts()[tap_stream_id],
                             record_count_by_stream_2[tap_stream_id])
