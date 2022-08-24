import logging
import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner
import os
import unittest
import string
import random
import paramiko
from datetime import timedelta
import csv
from datetime import datetime, timedelta, timezone
from stat import S_ISDIR
from singer import utils

RECORD_COUNT = {}

class TestSFTPBase(unittest.TestCase):
    START_DATE = ""

    def get_test_connection(self):
        username = os.getenv('TAP_SFTP_USERNAME')
        password = os.getenv('TAP_SFTP_PASSWORD')
        host = os.getenv('TAP_SFTP_HOST')
        port = os.getenv('TAP_SFTP_PORT')

        transport = paramiko.Transport((host, int(port)))
        transport.use_compression(True)
        transport.connect(username = username, password = password, hostkey = None, pkey = None)
        return paramiko.SFTPClient.from_transport(transport)

    def random_string_generator(self, size=6, chars=string.ascii_uppercase + string.digits):
        return ''.join(random.choice(chars) for x in range(size))

    def generate_simple_csv_lines_typeA(self, num_lines):
        lines = []
        for int_value in range(num_lines):
            lines.append([int_value, self.random_string_generator(), int_value*5])
        return lines

    def generate_simple_csv_lines_typeB(self, num_lines):
        lines = []
        start_datetime = datetime(2018, 1, 1, 19, 29, 14, 578000, tzinfo=timezone.utc)
        for int_value in range(num_lines):
            start_datetime = start_datetime + timedelta(days=5)
            lines.append([int_value, self.random_string_generator(), utils.strftime(start_datetime), int_value + random.random()])
        return lines

    def generate_simple_csv_lines_typeC(self, num_lines):
        lines = []
        start_datetime = datetime(2018, 1, 1, 19, 29, 14, 578000, tzinfo=timezone.utc)
        for int_value in range(num_lines):
            start_datetime = start_datetime + timedelta(days=5)
            lines.append([int_value, self.random_string_generator(), int_value*5, utils.strftime(start_datetime), int_value + random.random()])
        return lines

    def isdir(path, client):
        try:
            return S_ISDIR(client.stat(path).st_mode)
        except IOError:
            return False

    def rm(path, client):
        files = client.listdir(path=path)

        for f in files:
            filepath = os.path.join(path, f)
            if TestSFTPBase.isdir(filepath, client):
                TestSFTPBase.rm(filepath, client)
            else:
                client.remove(filepath)

        client.rmdir(path)

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

    def tap_name(self):
        return "tap-sftp"

    def get_type(self):
        return "platform.sftp"

    def get_credentials(self):
        return {'password': os.getenv('TAP_SFTP_PASSWORD')}

    def append_to_files(self, files_list=None):
        root_dir = os.getenv('TAP_SFTP_ROOT_DIR')

        with self.get_test_connection() as client:
            client.chdir(root_dir + '/tap_tester')

            # Append stuff to a subset of the files
            file_group = self.get_files()[0]
            headers = file_group['headers']
            directory = file_group['directory']
            for filename in file_group['files'] if files_list is None else files_list:
                client.chdir(directory)
                with client.open(filename, 'a') as f:
                    writer = csv.writer(f)
                    # appending greater number of rows as it takes some time
                    # so we can test modified date
                    lines = file_group['generator'](1500 if files_list else 10)
                    writer.writerows(lines)
                client.chdir('..')

    def get_common_properties(self):
        return {'start_date' : '2017-01-01T00:00:00Z',
                    'host' : os.getenv('TAP_SFTP_HOST'),
                    'port' : os.getenv('TAP_SFTP_PORT'),
                    'username' : os.getenv('TAP_SFTP_USERNAME'),
                    'private_key_file': None }

    def run_test(self):
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
            schema_and_metadata = menagerie.get_annotated_schema(conn_id, found_stream['stream_id'])
            main_metadata = schema_and_metadata["metadata"]
            stream_metadata = [mdata for mdata in main_metadata if mdata["breadcrumb"] == []]

            # assert that the pks are correct
            self.assertEqual(self.expected_pks()[tap_stream_id],
                                set(stream_metadata[0]['metadata']['table-key-properties']))

        for stream_catalog in catalog:
            annotated_schema = menagerie.get_annotated_schema(conn_id, stream_catalog['stream_id'])
            selected_metadata = connections.select_catalog_and_fields_via_metadata(conn_id,
                                                                                    stream_catalog,
                                                                                    annotated_schema['annotated-schema'],
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

    # helper functions
    def run_and_verify_check_mode(self, conn_id):
        """
        Run the tap in check mode and verify it succeeds.
        This should be ran prior to field selection and initial sync.
        Return the connection id and found catalogs from menagerie.
        """
        # run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # verify check exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(found_catalogs), 0, msg="unable to locate schemas for connection {}".format(conn_id))

        found_catalog_names = set(map(lambda c: c['stream_name'], found_catalogs))
        logging.info(found_catalog_names)
        self.assertSetEqual(self.expected_check_streams(), found_catalog_names, msg="discovered schemas do not match")
        logging.info("discovered schemas are OK")

        return found_catalogs

    def run_and_verify_sync(self, conn_id, second_sync = False):
        """
        Run a sync job and make sure it exited properly.
        Return a dictionary with keys of streams synced
        and values of records synced for each stream
        """
        # Run a sync job using orchestrator
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # Verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # Verify actual rows were synced
        sync_record_count = runner.examine_target_output_file(
            self, conn_id, self.expected_first_sync_streams() if not second_sync else self.expected_second_sync_streams(), self.expected_pks())
        self.assertGreater(
            sum(sync_record_count.values()), 0,
            msg="failed to replicate any data: {}".format(sync_record_count)
        )
        logging.info("total replicated row count: {}".format(sum(sync_record_count.values())))

        return sync_record_count

    def perform_and_verify_table_and_field_selection(self,
                                                     conn_id,
                                                     test_catalogs,
                                                     select_all_fields=True):
        """
        Perform table and field selection based off of the streams to select
        set and field selection parameters.
        Verify this results in the expected streams selected and all or no
        fields selected for those streams.
        """

        # Select all available fields or select no fields from all testable streams
        self.select_all_streams_and_fields(
            conn_id=conn_id, catalogs=test_catalogs, select_all_fields=select_all_fields
        )

        catalogs = menagerie.get_catalogs(conn_id)

        # Ensure our selection affects the catalog
        expected_selected = [tc.get('stream_name') for tc in test_catalogs]
        for cat in catalogs:
            catalog_entry = menagerie.get_annotated_schema(conn_id, cat['stream_id'])

            # Verify all testable streams are selected
            selected = catalog_entry.get('annotated-schema').get('selected')
            logging.info("Validating selection on {}: {}".format(cat['stream_name'], selected))
            if cat['stream_name'] not in expected_selected:
                self.assertFalse(selected, msg="Stream selected, but not testable.")
                continue # Skip remaining assertions if we aren't selecting this stream
            self.assertTrue(selected, msg="Stream not selected.")

            if select_all_fields:
                # Verify all fields within each selected stream are selected
                for field, field_props in catalog_entry.get('annotated-schema').get('properties').items():
                    field_selected = field_props.get('selected')
                    logging.info("\tValidating selection on {}.{}: {}".format(
                        cat['stream_name'], field, field_selected))
                    self.assertTrue(field_selected, msg="Field not selected.")
            else:
                # Verify only automatic fields are selected
                expected_automatic_fields = self.expected_pks().get(cat['stream_name'])
                selected_fields = self.get_selected_fields_from_metadata(catalog_entry['metadata'])
                self.assertEqual(expected_automatic_fields, selected_fields)

    @staticmethod
    def get_selected_fields_from_metadata(metadata):
        selected_fields = set()
        for field in metadata:
            is_field_metadata = len(field['breadcrumb']) > 1
            inclusion_automatic_or_selected = (
                field['metadata']['selected'] is True or \
                field['metadata']['inclusion'] == 'automatic'
            )
            if is_field_metadata and inclusion_automatic_or_selected:
                selected_fields.add(field['breadcrumb'][1])
        return selected_fields


    @staticmethod
    def select_all_streams_and_fields(conn_id, catalogs, select_all_fields: bool = True):
        """Select all streams and all fields within streams"""
        for catalog in catalogs:
            schema = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])

            non_selected_properties = []
            if not select_all_fields:
                # get a list of all properties so that none are selected
                non_selected_properties = schema.get('annotated-schema', {}).get(
                    'properties', {}).keys()

            connections.select_catalog_and_fields_via_metadata(
                conn_id, catalog, schema, [], non_selected_properties)
