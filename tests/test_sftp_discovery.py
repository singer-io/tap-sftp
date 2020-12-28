import os
import unittest
import string
import random
import time
import re
import pprint
import pdb
import paramiko
import csv
import json
from datetime import datetime, timedelta, timezone
from stat import S_ISDIR
from functools import reduce
import decimal
from singer import utils, metadata

import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner


def expected_catalog():
    return {
        "streams": [
            {
                "schema": {
                    "properties": {
                        "string_col": {
                            "type": [
                                "null",
                                "string"
                            ]
                        },
                        "_sdc_source_file": {
                            "type": "string"
                        },
                        "integer_col": {
                            "type": [
                                "null",
                                "integer",
                                "string"
                            ]
                        },
                        "_sdc_extra": {
                            "items": {
                                "type": "string"
                            },
                            "type": "array"
                        },
                        "id": {
                            "type": [
                                "null",
                                "integer",
                                "string"
                            ]
                        },
                        "_sdc_source_lineno": {
                            "type": "integer"
                        }
                    },
                    "type": "object"
                },
                "tap_stream_id": "table_1",
                "stream": "table_1"
            },
            {
                "schema": {
                    "properties": {
                        "string_col": {
                            "type": [
                                "null",
                                "string"
                            ]
                        },
                        "_sdc_source_file": {
                            "type": "string"
                        },
                        "_sdc_extra": {
                            "items": {
                                "type": "string"
                            },
                            "type": "array"
                        },
                        "id": {
                            "type": [
                                "null",
                                "integer",
                                "string"
                            ]
                        },
                        "datetime_col": {
                            "anyOf": [
                                {
                                    "format": "date-time",
                                    "type": [
                                        "null",
                                        "string"
                                    ]
                                },
                                {
                                    "type": [
                                        "null",
                                        "string"
                                    ]
                                }
                            ]
                        },
                        "number_col": {
                            "type": [
                                "null",
                                "number",
                                "string"
                            ]
                        },
                        "_sdc_source_lineno": {
                            "type": "integer"
                        }
                    },
                    "type": "object"
                },
                "tap_stream_id": "table_2",
                "stream": "table_2"
            },
            {
                "schema": {
                    "properties": {
                        "string_col": {
                            "type": [
                                "null",
                                "string"
                            ]
                        },
                        "_sdc_source_file": {
                            "type": "string"
                        },
                        "integer_col": {
                            "type": [
                                "null",
                                "integer",
                                "string"
                            ]
                        },
                        "_sdc_extra": {
                            "items": {
                                "type": "string"
                            },
                            "type": "array"
                        },
                        "id": {
                            "type": [
                                "null",
                                "integer",
                                "string"
                            ]
                        },
                        "datetime_col": {
                            "anyOf": [
                                {
                                    "format": "date-time",
                                    "type": [
                                        "null",
                                        "string"
                                    ]
                                },
                                {
                                    "type": [
                                        "null",
                                        "string"
                                    ]
                                }
                            ]
                        },
                        "number_col": {
                            "type": [
                                "null",
                                "number",
                                "string"
                            ]
                        },
                        "_sdc_source_lineno": {
                            "type": "integer"
                        }
                    },
                    "type": "object"
                },
                "tap_stream_id": "table_3",
                "stream": "table_3"
            }
        ]
    }

def get_test_connection():
    username = os.getenv('TAP_SFTP_USERNAME')
    password = os.getenv('TAP_SFTP_PASSWORD')
    host = os.getenv('TAP_SFTP_HOST')
    port = os.getenv('TAP_SFTP_PORT')

    # TODO: This test only uses username / password auth
    transport = paramiko.Transport((host, int(port)))
    transport.use_compression(True)
    transport.connect(username = username, password = password, hostkey = None)
    return paramiko.SFTPClient.from_transport(transport)

def random_string_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for x in range(size))

def generate_simple_csv_lines_typeA(num_lines):
    lines = []
    for int_value in range(num_lines):
        lines.append([int_value, random_string_generator(), int_value*5])
    return lines

def generate_simple_csv_lines_typeB(num_lines):
    lines = []
    start_datetime = datetime(2018, 1, 1, 19, 29, 14, 578000, tzinfo=timezone.utc)
    for int_value in range(num_lines):
        start_datetime = start_datetime + timedelta(days=5)
        lines.append([int_value, random_string_generator(), utils.strftime(start_datetime), int_value + random.random()])
    return lines

class SftpDiscovery(unittest.TestCase):
    def isdir(path, client):
        try:
            return S_ISDIR(client.stat(path).st_mode)
        except IOError:
            return False

    def rm(path, client):
        files = client.listdir(path=path)

        for f in files:
            filepath = os.path.join(path, f)
            if SftpDiscovery.isdir(filepath, client):
                SftpDiscovery.rm(filepath, client)
            else:
                client.remove(filepath)

        client.rmdir(path)

    def get_files(self):
        return [
            {
                "headers": ['id', 'string_col', 'integer_col'],
                "directory": "table_1_files",
                "files": ["table_1_fileA.csv", "table_3_fileA.csv"],
                "num_rows": 50,
                "generator": generate_simple_csv_lines_typeA
            },
            {
                "headers": ['id', 'string_col', 'datetime_col', 'number_col'],
                "directory": "table_2_files",
                "files": ["table_2_fileA.csv", "table_2_fileB.csv", "table_3_fileB.csv"],
                "num_rows": 50,
                "generator": generate_simple_csv_lines_typeB
            },
            {
                "headers": ['id', 'string_col', 'integer_col'],
                "directory": "table_3_files",
                "files": ["table_3_fileC.csv"],
                "num_rows": 50,
                "generator": generate_simple_csv_lines_typeA
            },
        ]


    def setUp(self):
        if not all([x for x in [os.getenv('TAP_SFTP_USERNAME'),
                                os.getenv('TAP_SFTP_PASSWORD'),
                                os.getenv('TAP_SFTP_ROOT_DIR')]]):
            #pylint: disable=line-too-long
            raise Exception("set TAP_SFTP_USERNAME, TAP_SFTP_PASSWORD, TAP_SFTP_ROOT_DIR")

        root_dir = os.getenv('TAP_SFTP_ROOT_DIR')

        with get_test_connection() as client:
            # drop all csv files in root dir
            client.chdir(root_dir)
            try:
                SftpDiscovery.rm('tap_tester', client)
            except FileNotFoundError:
                pass
            client.mkdir('tap_tester')

            # Add subdirectories
            client.mkdir('tap_tester/table_1_files')
            client.mkdir('tap_tester/table_2_files')
            client.mkdir('tap_tester/table_3_files')

            # Add csv files

            client.chdir('tap_tester')
            # Table 1 exists only in 'table_1_files' directory and has id, integer, string columns
            # Table 2 exists only in 'table_2_files' directory and has id, string, datetime, number columns
            # Table 3 exists both in 'table_1_files' AND 'table_2_files' directory and has id, integer, string, datetime, number columns

            # add multiple csv files for at least two different tables
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

    def expected_check_streams(self):
        return {
            'table_1',
            'table_2',
            'table_3'
        }

    def expected_pks(self):
        return {
            'table_1': {'id'},
            'table_2': {'id'},
            'table_3': {'id'}
        }

    def expected_columns(self):
        return {
            'table_1': {'id', 'integer_col', 'string_col'},
            'table_2': {'id', 'string_col', 'datetime_col', 'number_col'},
            'table_3': {'id', 'integer_col', 'string_col', 'datetime_col', 'number_col'},
        }

    def name(self):
        return "tap_tester_sftp_discovery"

    def tap_name(self):
        return "tap-sftp"

    def get_type(self):
        return "platform.sftp"

    def get_credentials(self):
        return {'password': os.getenv('TAP_SFTP_PASSWORD')}

    def get_properties(self):
        return {
            'start_date' : '2017-01-01T00:00:00Z',
            'host' : os.getenv('TAP_SFTP_HOST'),
            'port' : os.getenv('TAP_SFTP_PORT'),
            'username' : os.getenv('TAP_SFTP_USERNAME'),
            'tables': json.dumps([
                {
                    "table_name": "table_1",
                    "delimiter": ",",
                    "search_prefix": "upload/tap_tester/table_1_files",
                    "search_pattern": "table_1.*csv",
                    "key_properties": ['id']
                },
                {
                    "table_name": "table_2",
                    "delimiter": ",",
                    "search_prefix": "upload/tap_tester/table_2_files",
                    "search_pattern": "table_2.*csv",
                    "key_properties": ['id'],
                    "date_overrides": ["datetime_col"]
                },
                {
                    "table_name": "table_3",
                    "delimiter": ",",
                    "search_prefix": "upload/tap_tester",
                    "search_pattern": "table_3.*csv",
                    "key_properties": ['id'],
                    "date_overrides": ["datetime_col"]
                }
            ])
        }

    def test_run(self):
        conn_id = connections.ensure_connection(self)

        # TODO: Missing step here of adding to SFTP ~/.ssh/authorized_hosts
        
        # run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # check exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        # tap discovered the right streams
        catalog = menagerie.get_catalog(conn_id)

        # assert we find the correct streams
        self.assertEqual(self.expected_check_streams(),
                         {c['tap_stream_id'] for c in catalog['streams']})

        for tap_stream_id in self.expected_check_streams():
            found_stream = [c for c in catalog['streams'] if c['tap_stream_id'] == tap_stream_id][0]
            stream_metadata = [x['metadata'] for x in found_stream['metadata'] if x['breadcrumb']==[]][0]

            # table-key-properties metadata
            self.assertEqual(self.expected_pks()[tap_stream_id],
                             set(stream_metadata.get('table-key-properties')))

            # selected metadata is None for all streams
            self.assertNotIn('selected', stream_metadata.keys())

            # no forced-replication-method metadata
            self.assertEqual('INCREMENTAL', stream_metadata.get('forced-replication-method'))

            self.assertTrue(self.expected_columns()[tap_stream_id]
                            .issubset(set(found_stream['schema']['properties'].keys())))

            expected_stream = [c for c in expected_catalog()['streams'] if c['tap_stream_id'] == tap_stream_id][0]

            # Pop the annotated schema because that isn't found in discovery
            found_stream['schema'].pop('inclusion')
            for col_names, col_props in found_stream['schema']['properties'].items():
                col_props.pop('inclusion')
            # compare schemas
            self.assertEqual(expected_stream['schema'], found_stream['schema'])
