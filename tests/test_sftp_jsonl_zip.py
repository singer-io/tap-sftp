from base import TestSFTPBase
import os
import json
import zipfile

class TestSFTPZipJsonl(TestSFTPBase):

    def name(self):
        return "tap_tester_sftp_jsonl_zip"

    def get_files(self):
        return [
            {
                "headers": ['id', 'string_col', 'integer_col'],
                "directory": "folderA",
                "files": ["table_1_fileA.jsonl", "table_3_fileA.jsonl"],
                "archive": 'table_1.zip',
                "num_rows": 50,
                "generator": self.generate_simple_jsonl_lines_typeA
            },
            {
                "headers": ['id', 'string_col', 'datetime_col', 'number_col'],
                "directory": "folderB",
                "files": ["table_2_fileA.jsonl", "table_2_fileB.jsonl", "table_3_fileB.jsonl"],
                "archive": 'table_2.zip',
                "num_rows": 50,
                "generator": self.generate_simple_jsonl_lines_typeB
            },
            {
                "headers": ['id', 'string_col', 'integer_col'],
                "directory": "folderC",
                "files": ["table_3_fileC.jsonl"],
                "archive": 'table_3.zip',
                "num_rows": 50,
                "generator": self.generate_simple_jsonl_lines_typeA
            },
        ]

    def expected_first_sync_row_counts(self):
        return {
            'table_1': 100,
            'table_2': 150,
            'table_3': 50
        }

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
                TestSFTPZipJsonl.rm('tap_tester', client)
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
                directory = file_group['directory']
                client.chdir(directory)
                with client.open(file_group['archive'], 'w') as direct_file:
                    with zipfile.ZipFile(direct_file, mode='w') as zip_file:
                        lines = file_group['generator'](file_group['num_rows'])
                        total = ''
                        for line in lines:
                            total += json.dumps(line) + '\n'
                        for file_name in file_group['files']:
                            zip_file.writestr(file_name, total)
                client.chdir('..')

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
        self.run_test()
