from base import TestSFTPBase
import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner
import os
import json
import random
import string
import zipfile

from datetime import datetime, timedelta, timezone
from singer import utils

RECORD_COUNT = {}
records = {}
class TestSFTPEncoding(TestSFTPBase):

    def name(self):
        return "tap_tester_sftp_encoding"

    def random_string_generator(self, encoding_format, size=6):
        # standard character set
        alphanumeric_chars = string.ascii_uppercase + string.digits

        # special character set
        special_chars = {
            "latin-1": "áéíóúüñçàè"
        }.get(encoding_format, "@!$%^&")

        return "".join(random.choice(alphanumeric_chars) for x in range(size-2)) + "".join(random.choice(special_chars) for x in range(2))

    def generate_encoded_csv_lines(self, num_lines, table_name, encoding_format="utf-8"):
        lines = []
        for int_value in range(num_lines):
            lines.append(
                [int_value, self.random_string_generator(encoding_format), int_value*5])
        records[table_name] = lines
        return lines

    def generate_encoded_csv_lines_with_datetime(self, num_lines, table_name, encoding_format="utf-8"):
        lines = []
        start_datetime = datetime(2018, 1, 1, 19, 29, 14, 578000, tzinfo=timezone.utc)
        for int_value in range(num_lines):
            start_datetime = start_datetime + timedelta(days=5)
            lines.append(
                [int_value, self.random_string_generator(encoding_format), utils.strftime(start_datetime), int_value*5])
        records[table_name] = lines
        return lines

    def get_files(self):
        # TODO: write down the generator function for the special characters
        return [
            {
                "headers": ["id", "string_col", "integer_col"],
                "directory": "table_1",
                "files": ["table_1.csv"],
                "archive": "table_1.zip",
                "num_rows": 50,
                "generator": self.generate_encoded_csv_lines,
                "encoding_format": "latin-1"
            },
            {
                "headers": ["id", "string_col", "datetime_col", "number_col"],
                "directory": "table_2",
                "files": ["table_2.csv"],
                "archive": "table_2.zip",
                "num_rows": 50,
                "generator": self.generate_encoded_csv_lines_with_datetime,
                "encoding_format": "latin-1"
            },
            {
                "headers": ["id", "string_col", "integer_col"],
                "directory": "table_3",
                "files": ["table_3.csv"],
                "archive": "table_3.zip",
                "num_rows": 50,
                "generator": self.generate_encoded_csv_lines,
                "encoding_format": "utf-8"
            },
        ]

    def get_headers_for_table(self, target_directory):
        for file_info in self.get_files():
            if file_info["directory"] == target_directory:
                return file_info["headers"]

    def setUp(self):
        if not all([x for x in [os.getenv("TAP_SFTP_USERNAME"),
                                os.getenv("TAP_SFTP_PASSWORD"),
                                os.getenv("TAP_SFTP_ROOT_DIR")]]):
            #pylint: disable=line-too-long
            raise Exception("set TAP_SFTP_USERNAME, TAP_SFTP_PASSWORD, TAP_SFTP_ROOT_DIR")

        root_dir = os.getenv("TAP_SFTP_ROOT_DIR")

        with self.get_test_connection() as client:
            # drop all csv files in root dir
            client.chdir(root_dir)
            try:
                TestSFTPEncoding.rm("tap_tester", client)
            except FileNotFoundError:
                pass
            client.mkdir("tap_tester")
            client.chdir("tap_tester")

            # Add subdirectories
            file_info = self.get_files()
            for entry in file_info:
                client.mkdir(entry["directory"])

            # Add csv files
            for file_group in file_info:
                headers = file_group["headers"]
                directory = file_group["directory"]
                encoding_format = file_group["encoding_format"]
                client.chdir(directory)
                with client.open(file_group["archive"], "w") as direct_file:
                    with zipfile.ZipFile(direct_file, mode="w") as zip_file:
                        lines = [headers] + file_group["generator"](file_group["num_rows"], directory, encoding_format)
                        total = ""
                        for line in lines:
                            total += ",".join((str(val) for val in line)) + "\n"
                        for file_name in file_group["files"]:
                            zip_file.writestr(file_name, total.encode(encoding_format))
                client.chdir("..")

    def expected_first_sync_row_counts(self):
        return {
            "table_1": 50,
            "table_2": 50,
            "table_3": 50
        }

    def get_properties(self):
        props = self.get_common_properties()
        props["tables"] = json.dumps([
                {
                    "table_name": "table_1",
                    "search_prefix": os.getenv("TAP_SFTP_ROOT_DIR") + "/tap_tester",
                    "delimiter": ",",
                    "search_pattern": "table_1\.zip",
                    "key_properties": ["id"],
                },
                {
                    "table_name": "table_2",
                    "search_prefix": os.getenv("TAP_SFTP_ROOT_DIR") + "/tap_tester",
                    "delimiter": ",",
                    "search_pattern": "table_2\.zip",
                    "key_properties": ["id"],
                    "date_overrides": ["datetime_col"]
                },
                {
                    "table_name": "table_3",
                    "search_prefix": os.getenv("TAP_SFTP_ROOT_DIR") + "/tap_tester",
                    "delimiter": ",",
                    "search_pattern": "table_3\.zip",
                    "key_properties": ["id"],
                    "date_overrides": ["datetime_col"]
                }
            ])
        props["encoding_format"] = "latin-1"
        return props

    def test_run(self):
        conn_id = connections.ensure_connection(self)

        # run in discovery mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # verify check  exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        # verify the tap discovered the right streams
        catalog = menagerie.get_catalogs(conn_id)
        found_catalog_names = set(map(lambda c: c["tap_stream_id"], catalog))

        # assert we find the correct streams
        self.assertEqual(self.expected_check_streams(),
                         found_catalog_names)

        for tap_stream_id in self.expected_check_streams():
            with self.subTest(stream=tap_stream_id):
                found_stream = [c for c in catalog if c["tap_stream_id"] == tap_stream_id][0]

                schema_and_metadata = menagerie.get_annotated_schema(conn_id, found_stream["stream_id"])
                main_metadata = schema_and_metadata["metadata"]
                stream_metadata = [mdata for mdata in main_metadata if mdata["breadcrumb"] == []][0]

                # table-key-properties metadata
                self.assertEqual(self.expected_pks()[tap_stream_id],
                                 set(stream_metadata["metadata"]["table-key-properties"]))


        for stream_catalog in catalog:
            annotated_schema = menagerie.get_annotated_schema(conn_id, stream_catalog["stream_id"])
            selected_metadata = connections.select_catalog_and_fields_via_metadata(conn_id,
                                                                                   stream_catalog,
                                                                                   annotated_schema,
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

        # Verify that the full table was synced
        for tap_stream_id in self.expected_first_sync_streams():
            expected_row_count = self.expected_first_sync_row_counts()[tap_stream_id]
            actual_row_count = record_count_by_stream[tap_stream_id]
            self.assertEqual(expected_row_count, actual_row_count)

            # Verify that the records match after extraction
            initial_records = records[tap_stream_id]
            extracted_messages = messages_by_stream[tap_stream_id]["messages"]
            headers = self.get_headers_for_table(tap_stream_id)
            
            for i in range(0, len(initial_records)):
                extracted_record = [extracted_messages[i]["data"][key] for key in headers]
                self.assertEqual(initial_records[i], extracted_record)

