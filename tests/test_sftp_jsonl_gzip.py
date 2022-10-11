from base import TestSFTPBase
import os
import json
import gzip
import io

class TestSFTPGzip(TestSFTPBase):

    def name(self):
        return "tap_tester_sftp_gzip"

    def get_files(self):
        return [
            {
                "headers": ['id', 'string_col', 'integer_col'],
                "directory": "folderA",
                "files": ["table_1_fileA.jsonl.gz", "table_3_fileA.jsonl.gz"],
                "num_rows": 50,
                "generator": self.generate_simple_jsonl_lines_typeA
            },
            {
                "headers": ['id', 'string_col', 'datetime_col', 'number_col'],
                "directory": "folderB",
                "files": ["table_2_fileA.jsonl.gz", "table_2_fileB.jsonl.gz", "table_3_fileB.jsonl.gz"],
                "num_rows": 50,
                "generator": self.generate_simple_jsonl_lines_typeB
            },
            {
                "headers": ['id', 'string_col', 'integer_col', 'datetime_col', 'number_col'],
                "directory": "folderC",
                "files": ["table_3_fileC.jsonl.gz"],
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
                directory = file_group['directory']
                for filename in file_group['files']:
                    file_to_gzip = ".".join(filename.split(".")[:-1])
                    client.chdir(directory)
                    with client.open(filename, 'w') as direct_file:
                        with gzip.GzipFile(filename=file_to_gzip, fileobj=direct_file, mode='w') as gzip_file:
                            with io.TextIOWrapper(gzip_file, encoding='utf-8') as f:
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