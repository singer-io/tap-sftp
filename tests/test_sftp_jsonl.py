from base import TestSFTPBase
import os
import json

class TestSFTPJsonl(TestSFTPBase):

    def name(self):
        return "tap_tester_sftp_jsonl"

    def get_files(self):
        return [
            {
                "directory": "folderA",
                "files": ["table_1_fileA.jsonl", "table_3_fileA.jsonl"],
                "num_rows": 50,
                "generator": self.generate_simple_jsonl_lines_typeA
            },
            {
                "directory": "folderB",
                "files": ["table_2_fileA.jsonl", "table_2_fileB.jsonl", "table_3_fileB.jsonl"],
                "num_rows": 50,
                "generator": self.generate_simple_jsonl_lines_typeB
            },
            {
                "directory": "folderC",
                "files": ["table_3_fileC.jsonl"],
                "num_rows": 50,
                "generator": self.generate_simple_jsonl_lines_typeC
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
                TestSFTPJsonl.rm('tap_tester', client)
            except FileNotFoundError:
                pass
            client.mkdir('tap_tester')
            client.chdir('tap_tester')

            # Add subdirectories
            file_info = self.get_files()
            for entry in file_info:
                client.mkdir(entry['directory'])

            for file_group in file_info:
                directory = file_group['directory']
                for filename in file_group['files']:
                    client.chdir(directory)
                    with client.open(filename, 'w') as f:
                        for record in file_group['generator'](file_group['num_rows']):
                            f.write(json.dumps(record) + "\n")
                    client.chdir('..')

    def get_properties(self):
        props = self.get_common_properties()
        props['tables'] = json.dumps([
                {
                    "table_name": "table_1",
                    "delimiter": ",",
                    "search_prefix": os.getenv("TAP_SFTP_ROOT_DIR") + "/tap_tester",
                    "search_pattern": "table_1.*jsonl",
                    "key_properties": ['id']
                },
                {
                    "table_name": "table_2",
                    "delimiter": ",",
                    "search_prefix": os.getenv("TAP_SFTP_ROOT_DIR") + "/tap_tester",
                    "search_pattern": "table_2.*jsonl",
                    "key_properties": ['id'],
                    "date_overrides": ["datetime_col"]
                },
                {
                    "table_name": "table_3",
                    "delimiter": ",",
                    "search_prefix": os.getenv("TAP_SFTP_ROOT_DIR") + "/tap_tester",
                    "search_pattern": "table_3.*jsonl",
                    "key_properties": ['id'],
                    "date_overrides": ["datetime_col"]
                }
            ])
        return props

    def test_run(self):
        self.run_test()
