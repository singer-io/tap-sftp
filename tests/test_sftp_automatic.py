from base import TestSFTPBase
import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner
import os
import json

class TestSFTPAutomatic(TestSFTPBase):
    """Test case to verify we are replicating automatic fields data when all the fields are not selected"""

    def name(self):
        """Returns name of the test"""
        return "tap_tester_sftp_automatic_fields"

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
        """Expected streams"""
        return {
            'table_1',
        }

    def expected_pks(self):
       """Expected primary keys"""
       return {
            'table_1': {'id'},
        }

    def expected_automatic_fields(self):
        """Expected automatic fields"""
        return {
            'table_1': {'id'},
        }

    def test_run(self):

        conn_id = connections.ensure_connection(self)
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Clear state before our run
        menagerie.set_state(conn_id, {})
        
        expected_streams =  self.expected_sync_streams()
        test_catalogs_automatic_fields = [catalog for catalog in found_catalogs
                                          if catalog.get('stream_name') in expected_streams]

        self.perform_and_verify_table_and_field_selection(
            conn_id, test_catalogs_automatic_fields, select_all_fields=False,
        )

        # Run a sync job using orchestrator
        self.run_and_verify_sync(conn_id)

        synced_records = runner.get_records_from_target_output()

        for stream in self.expected_sync_streams():
            with self.subTest(stream=stream):

                expected_keys = self.expected_automatic_fields().get(stream)

                # Collect actual values
                data = synced_records.get(stream, {})
                record_messages_keys = [set(row.get('data').keys()) for row in data.get('messages', {})]

                for actual_keys in record_messages_keys:
                    self.assertSetEqual(expected_keys, actual_keys)
