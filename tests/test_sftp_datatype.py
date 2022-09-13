import logging
from base import TestSFTPBase
from datetime import datetime, timezone
from decimal import Decimal
from functools import reduce
import os
import json
from singer import utils
from tap_tester import connections, menagerie, runner, LOGGER


class TestSFTPDatatype(TestSFTPBase):    
    """Test case to verify tap is writing data with appropriate type """
    def name(self):
        """Returns name of the test"""
        return "tap_tester_sftp_Datatype"

    def generate_records(self, num_lines):
        """Generates records for files"""
        lines = []
        for int_value in range(num_lines):
            start_datetime = datetime(2018, 1, 1, 19, 29, 14, 578000, tzinfo=timezone.utc)
            lines.append([int_value, 'test_data' , int_value*5, utils.strftime(start_datetime), ["data", "of", "string"],{"key1": "value1", "key2": "value2"},1.22])
        return lines

    def get_files(self):
        """Generate files for the test"""
        return [
            {
                "headers": ['id', 'string_col', 'integer_col', 'date_col', "list_string", "dict_string","float_col"],
                "directory": "table_1_files",
                "files": ["table_1_fileA.csv"],
                "num_rows": 1,
                "generator": self.generate_records
            },
        ]

    def setUp(self):
        """Setup the directory for test """
        self.add_dir()

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
    
    def expected_sync_streams(self):
        """Expected sync streams"""
        return {
            'table_1',
        }
    def expected_check_streams(self):
        """Expected check streams"""
        return {
            'table_1',
        }
    def expected_pks(self):
        """Expected primary keys"""
        return {
            'table_1': {"id"},
        }

    def expected_sync_row_counts(self):
        """Expected row count"""
        return {
            'table_1': 1,
        }

    def expected_data(self):
        """Expected data"""
        return {
            'id': 0,
            'string_col': 'test_data',
            'integer_col': 0,
            "date_col": "2018-01-01T19:29:14.578000Z",
            "list_string": "['data', 'of', 'string']" ,
            "dict_string":"{'key1': 'value1', 'key2': 'value2'}",
            'float_col': Decimal('1.22'),
            '_sdc_source_lineno': 2,
        }


    def test_discovery_run(self):
        """
        - Verify we discovered correct streams.
        - Verify the Primary keys are correct.
        """
        conn_id = connections.ensure_connection(self)
        # Run in discovery mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # Verify check exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        # Verify the tap discovered the right streams
        found_catalogs = menagerie.get_catalogs(conn_id)

        # Assert we find the correct streams
        self.assertEqual(self.expected_check_streams(),
                         {c['tap_stream_id'] for c in found_catalogs})
        
        for tap_stream_id in self.expected_check_streams():
            found_stream = [c for c in found_catalogs if c['tap_stream_id'] == tap_stream_id][0]

            # Assert that the pks are correct
            self.assertEqual(self.expected_pks()[found_stream['stream_name']],
                             set(found_stream.get('metadata', {})[0].get('metadata', {}).get('table-key-properties')))

    def test_run_sync_mode(self):
        """
        - Verify actual rows were synced
        - Verify the record data matches the expected data
        """
        conn_id = connections.ensure_connection(self)
        
        # Select our catalogs
        our_catalogs = self.run_and_verify_check_mode(conn_id)
        self.perform_and_verify_table_and_field_selection(conn_id, our_catalogs, True)

        # Clear state before our run
        menagerie.set_state(conn_id, {})

        # Run a sync job using orchestrator
        self.run_and_verify_sync(conn_id)

        # Verify actual rows were synced
        record_count_by_stream = runner.examine_target_output_file(
            self,
            conn_id,
            self.expected_check_streams(),
            self.expected_pks())
        replicated_row_count = reduce(lambda accum, c : accum + c, record_count_by_stream.values())

        for stream in self.expected_check_streams():
            with self.subTest(stream=stream):
                self.assertEqual(
                    record_count_by_stream.get(stream, 0),
                    self.expected_sync_row_counts()[stream],
                    msg="actual rows: {}, expected_rows: {} for stream {} don't match".format(
                        record_count_by_stream.get(stream, 0),
                        self.expected_sync_row_counts()[stream],
                        stream)
                )

        LOGGER.info("total replicated row count: {}".format(replicated_row_count))

        messages_by_stream = runner.get_records_from_target_output()

        # Verify the record data matches the expected data
        for stream in self.expected_check_streams():
            records = [record.get("data") for record in messages_by_stream.get(stream, {}).get("messages", [])
                       if record.get("action") == "upsert"]
            self.assertEqual(len(records), 1)
            for record in records:
                del record["_sdc_source_file"]
            self.assertEqual([self.expected_data()], records)
    
    def test_primary_keys(self):
        """
        - Verify that expected_key_properties show as automatic in metadata
        """
        conn_id = connections.ensure_connection(self)
        found_catalogs = self.run_and_verify_check_mode(conn_id)
        all_catalogs = [x for x in found_catalogs]
        for catalog in all_catalogs:
            with self.subTest(c=catalog):
                expected_key_properties = \
                    self.expected_pks()[catalog["stream_name"]]
                metadata_and_annotated_schema = menagerie.get_annotated_schema(
                    conn_id, catalog['stream_id'])

                # Verify that expected_key_properties show as automatic in metadata
                metadata = metadata_and_annotated_schema["metadata"]
                actual_key_properties = {item.get("breadcrumb", ["", ""])[1]
                                         for item in metadata
                                         if item.get("metadata").get("inclusion") == "automatic"}
                self.assertEqual(actual_key_properties, expected_key_properties)
