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


RECORD_COUNT = {}

def get_test_connection():
    username = os.getenv('TAP_SFTP_USERNAME')
    password = os.getenv('TAP_SFTP_PASSWORD')
    host = os.getenv('TAP_SFTP_HOST')
    port = os.getenv('TAP_SFTP_PORT')

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

class SftpSync(unittest.TestCase):
    def isdir(path, client):
        try:
            return S_ISDIR(client.stat(path).st_mode)
        except IOError:
            return False

    def rm(path, client):
        files = client.listdir(path=path)

        for f in files:
            filepath = os.path.join(path, f)
            if SftpSync.isdir(filepath, client):
                SftpSync.rm(filepath, client)
            else:
                client.remove(filepath)

        client.rmdir(path)

    def get_files(self):
        return [
            {
                "headers": ['id', 'string_col', 'integer_col'],
                "directory": "folderA",
                "files": ["table_1_fileA.csv", "table_3_fileA.csv"],
                "num_rows": 50,
                "generator": generate_simple_csv_lines_typeA
            },
            {
                "headers": ['id', 'string_col', 'datetime_col', 'number_col'],
                "directory": "folderB",
                "files": ["table_2_fileA.csv", "table_2_fileB.csv", "table_3_fileB.csv"],
                "num_rows": 50,
                "generator": generate_simple_csv_lines_typeB
            },
            {
                "headers": ['id', 'string_col', 'integer_col'],
                "directory": "folderC",
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
                SftpSync.rm('tap_tester', client)
            except FileNotFoundError:
                pass
            client.mkdir('tap_tester')
            client.chdir('tap_tester')

            # Add subdirectories
            file_info = self.get_files()
            for entry in file_info:
                client.mkdir(entry['directory'])

            # Add csv files

            # Table 1 exists only in 'table_1_files' directory and has id, integer, string columns
            # Table 2 exists only in 'table_2_files' directory and has id, string, datetime, number columns
            # Table 3 exists both in 'table_1_files' AND 'table_2_files' directory and has id, integer, string, datetime, number columns

            # add multiple csv files for at least two different tables
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

    def expected_first_sync_row_counts(self):
        return {
            'table_1': 50,
            'table_2': 100,
            'table_3': 150
        }

    def expected_second_sync_row_counts(self):
        return {
            'table_1': 110,
            'table_2': 0,
            'table_3': 60
        }

    def expected_first_sync_streams(self):
        return {
            'table_1',
            'table_2',
            'table_3',
        }

    def expected_second_sync_streams(self):
        return {
            'table_1',
            'table_3',
        }

    def name(self):
        return "tap_tester_sftp_sync"

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
                    "search_prefix": "upload/tap_tester/folderA",
                    "search_pattern": "table_1.*csv",
                    "key_properties": ['id']
                },
                {
                    "table_name": "table_2",
                    "search_prefix": "upload/tap_tester/folderB",
                    "search_pattern": "table_2.*csv",
                    "delimiter": ",",
                    "key_properties": ['id'],
                    "date_overrides": ["datetime_col"]
                },
                {
                    "table_name": "table_3",
                    "search_prefix": "",
                    "search_pattern": "table_3.*csv",
                    "key_properties": ['id'],
                    "delimiter": ",",
                    "date_overrides": ["datetime_col"]
                }
            ])
        }

    def append_to_files(self):
        root_dir = os.getenv('TAP_SFTP_ROOT_DIR')

        with get_test_connection() as client:
            client.chdir(root_dir + '/tap_tester')

            # Append stuff to a subset of the files
            file_group = self.get_files()[0]
            headers = file_group['headers']
            directory = file_group['directory']
            for filename in file_group['files']:
                client.chdir(directory)
                with client.open(filename, 'a') as f:
                    writer = csv.writer(f)
                    lines = file_group['generator'](10)
                    writer.writerows(lines)
                client.chdir('..')


    def test_run(self):

        conn_id = connections.ensure_connection(self)

        #  -------------------------------
        # -----------  Discovery ----------
        #  -------------------------------

        # run in discovery mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # verify check  exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        # verify the tap discovered the right streams
        catalog = menagerie.get_catalog(conn_id)
        found_catalogs = menagerie.get_catalogs(conn_id)

        # assert we find the correct streams
        self.assertEqual(self.expected_check_streams(),
                         {c['tap_stream_id'] for c in catalog['streams']})


        for tap_stream_id in self.expected_check_streams():
            found_stream = [c for c in catalog['streams'] if c['tap_stream_id'] == tap_stream_id][0]
            stream_metadata = [x['metadata'] for x in found_stream['metadata'] if x['breadcrumb']==[]][0]

            # assert that the pks are correct
            self.assertEqual(self.expected_pks()[found_stream['stream']],
                             set(stream_metadata.get('table-key-properties')))

            # assert that the row counts are correct
            # self.assertEqual(self.expected_row_counts()[found_stream['stream']],
            #                  stream_metadata.get('row-count'))


        #  -----------------------------------
        # ----------- Initial Sync ---------
        #  -----------------------------------
        for stream_catalog in found_catalogs:
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

        # Verify that we have 'initial_full_table_complete' bookmark
        state = menagerie.get_state(conn_id)
        first_versions = {}

        # -----------------------------------
        # ------------ Second Sync ----------
        # -----------------------------------

        # Add table_1_fileB
        with get_test_connection() as client:
            root_dir = os.getenv('TAP_SFTP_ROOT_DIR')
            client.chdir(root_dir + '/tap_tester/folderA')

            file_group = self.get_files()[0]
            with client.open('table_1_fileB.csv', 'w') as f:
                writer = csv.writer(f)
                lines = [file_group['headers']] + file_group['generator'](file_group['num_rows'])
                writer.writerows(lines)

        # Update table_1_fileA and table_3_fileA by adding 10 rows to both
        self.append_to_files()

        # Run sync
        sync_job_name = runner.run_sync_mode(self, conn_id)
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # verify the persisted schema was correct
        messages_by_stream = runner.get_records_from_target_output()
        records_by_stream = {}
        for stream_name in self.expected_second_sync_streams():
            records_by_stream[stream_name] = [x for x in messages_by_stream[stream_name]['messages'] if x.get('action') == 'upsert']

        # assert that each of the streams that we synced are the ones that we expect to see
        record_count_by_stream = runner.examine_target_output_file(self,
                                                                   conn_id,
                                                                   self.expected_first_sync_streams(),
                                                                   self.expected_pks())

        # Verify that we got 3 records for each stream (2 because of the new records, 1 because
        # of gte)
        for k,v in record_count_by_stream.items():
            self.assertEqual(self.expected_second_sync_row_counts()[k], v)
