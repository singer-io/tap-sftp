import logging
from base import TestSFTPBase
import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner
import os
import csv
import json

class TestSFTPDiscovery(TestSFTPBase):
    """Test case to verify the Tap is creating the catalog file as expected"""

    def name(self):
        return "tap_tester_sftp_discovery"

    def setUp(self):
        """Setup the directory for test """
        self.add_dir()

    def get_properties(self):
        props = self.get_common_properties()
        props['tables'] = json.dumps([
                {
                    "table_name": "table_1",
                    "delimiter": ",",
                    "search_prefix": os.getenv("TAP_SFTP_ROOT_DIR") + "/tap_tester",
                    "search_pattern": "table_1.*csv",
                    "key_properties": ['id']
                },
                {
                    "table_name": "table_2",
                    "delimiter": ",",
                    "search_prefix": os.getenv("TAP_SFTP_ROOT_DIR") + "/tap_tester",
                    "search_pattern": "table_2.*csv",
                    "key_properties": ['id'],
                    "date_overrides": ["datetime_col"]
                },
                {
                    "table_name": "table_3",
                    "delimiter": ",",
                    "search_prefix": os.getenv("TAP_SFTP_ROOT_DIR") + "/tap_tester",
                    "search_pattern": "table_3.*csv",
                    "key_properties": ['id'],
                    "date_overrides": ["datetime_col"]
                }
            ])
        return props

    def test_run(self):
        conn_id = connections.ensure_connection(self)

        # Run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # Check exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        # Tap discovered the right streams
        catalog = menagerie.get_catalogs(conn_id)
        found_catalog_names = set(map(lambda c: c['tap_stream_id'], catalog))

        # Assert we find the correct streams
        self.assertEqual(self.expected_check_streams(),
                         found_catalog_names)

        for tap_stream_id in self.expected_check_streams():
            with self.subTest(stream=tap_stream_id):
                found_stream = [c for c in catalog if c['tap_stream_id'] == tap_stream_id][0]

                expected_primary_keys = self.expected_pks()[tap_stream_id]

                schema_and_metadata = menagerie.get_annotated_schema(conn_id, found_stream['stream_id'])
                main_metadata = schema_and_metadata["metadata"]
                stream_metadata = [mdata for mdata in main_metadata if mdata["breadcrumb"] == []][0]
                actual_primary_keys = set(stream_metadata.get("metadata", {"table-key-properties": []}).get("table-key-properties", []))
                automatic_fields = [mdata["breadcrumb"][1]
                                    for mdata in main_metadata
                                    if mdata["metadata"]["inclusion"] == "automatic"]

                # Table-key-properties metadata
                self.assertEqual(self.expected_pks()[tap_stream_id],
                                 set(stream_metadata["metadata"]["table-key-properties"]))

                # Replication method check
                self.assertEqual('INCREMENTAL',
                                 stream_metadata["metadata"]['forced-replication-method'])

                # Check if all columns are present or not
                self.assertTrue(
                    set(
                        self.expected_columns()[tap_stream_id]
                    ).issubset(
                        set(
                            schema_and_metadata['annotated-schema']['properties'].keys()
                        )
                    )
                )

                # Check if only primary key is "automatic"
                self.assertEqual(self.expected_pks()[tap_stream_id],
                                 set(automatic_fields))

                # Check all other fields are "available"
                self.assertTrue(
                        all({available_items["metadata"]["inclusion"] == "available"
                             for available_items in main_metadata
                             if available_items.get("breadcrumb", []) != []
                             and available_items.get("breadcrumb", ["properties", None])[1]
                             not in automatic_fields}),
                        msg="Not all non key properties are set to available in metadata")

                # Verify primary key(s) match expectations
                self.assertSetEqual(expected_primary_keys, actual_primary_keys)
                
                # Verify there is only 1 top level breadcrumb
                stream_properties = [item for item in main_metadata if item.get("breadcrumb") == []]
                self.assertTrue(len(stream_properties) == 1,
                                msg="There is more than one top level breadcrumb")

                actual_fields = []
                for md_entry in main_metadata:
                    if md_entry['breadcrumb'] != []:
                        actual_fields.append(md_entry['breadcrumb'][1])

                # Verify there are no duplicate metadata entries
                self.assertEqual(len(actual_fields), len(set(actual_fields)), msg = "duplicates in the metadata entries retrieved")