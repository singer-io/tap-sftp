from datetime import datetime, timezone
from decimal import Decimal
from base import TestSFTPBase
from tap_tester import connections, menagerie, runner
import os
import json
from singer import utils

class TestSFTPJsonlData(TestSFTPBase):

    def name(self):
        return "tap_tester_sftp_jsonl_data"

    def generate_jsonl_data(self):
        start_datetime = datetime(2018, 1, 1, 19, 29, 14, 578000, tzinfo=timezone.utc)
        return [{"int": 1, "string": "string_data", "float": 1.22, "date": utils.strftime(start_datetime)}]

    def generate_jsonl_dict_data(self):
        start_datetime_1 = datetime(2018, 1, 1, 19, 29, 14, 578000, tzinfo=timezone.utc)
        start_datetime_2 = datetime(2019, 1, 1, 19, 29, 14, 578000, tzinfo=timezone.utc)
        return [{
            "int": 1,
            "dict_int": {1: 1, 2: 2},
            "dict_float": {1.2: 2.0},
            "dict_string": {"key1": "value1", "key2": "value2"},
            "dict_dict": {"key": {"name": "john"}},
            "dict_list": {"ids": [1, 2, 3]},
            "dict_datetime": {"date_1": utils.strftime(start_datetime_1), "date_2": utils.strftime(start_datetime_2)}
        }]

    def generate_jsonl_list_data(self):
        start_datetime_1 = datetime(2018, 1, 1, 19, 29, 14, 578000, tzinfo=timezone.utc)
        start_datetime_2 = datetime(2019, 1, 1, 19, 29, 14, 578000, tzinfo=timezone.utc)
        return [{
            "int": 1,
            "list_int": [1, 2, 3],
            "list_float": [1.2, 2.3, 3.4],
            "list_string": ["data", "of", "string"],
            "list_dict": [{"id": 1}, {"id": 2}],
            "list_list": [[1, 2 , 3], [4, 5, 6]],
            "list_datetime": [utils.strftime(start_datetime_1), utils.strftime(start_datetime_2)]
        }]

    def get_files(self):
        return [
            {
                "directory": "mytable",
                "files": ["file1.jsonl"],
                "generator": self.generate_jsonl_data
            },
            {
                "directory": "mytable_list",
                "files": ["file2.jsonl"],
                "generator": self.generate_jsonl_list_data
            },
            {
                "directory": "mytable_dict",
                "files": ["file3.jsonl"],
                "generator": self.generate_jsonl_dict_data
            }
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
                TestSFTPJsonlData.rm('tap_tester', client)
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
                        for record in file_group['generator']():
                            f.write(json.dumps(record) + "\n")
                    client.chdir('..')

    def get_properties(self):
        props = self.get_common_properties()
        props['tables'] = json.dumps([
                {
                    "table_name": "mytable",
                    "delimiter": ",",
                    "search_prefix": os.getenv("TAP_SFTP_ROOT_DIR") + "/tap_tester",
                    "search_pattern": "file1.*jsonl",
                    "key_properties": []
                },
                {
                    "table_name": "mytable_list",
                    "delimiter": ",",
                    "search_prefix": os.getenv("TAP_SFTP_ROOT_DIR") + "/tap_tester",
                    "search_pattern": "file2.*jsonl",
                    "key_properties": []
                },
                {
                    "table_name": "mytable_dict",
                    "delimiter": ",",
                    "search_prefix": os.getenv("TAP_SFTP_ROOT_DIR") + "/tap_tester",
                    "search_pattern": "file3.*jsonl",
                    "key_properties": []
                }
            ])
        return props

    def expected_check_streams(self):
        return {"mytable", "mytable_dict", "mytable_list"}

    def expected_pks(self):
        return {
            "mytable": set(),
            "mytable_dict": set(),
            "mytable_list": set()
        }

    def expected_data(self):
        return {
            "mytable": {
                "int": 1,
                "string": "string_data",
                "float": Decimal(1.22),
                "date": "2018-01-01T19:29:14.578000Z",
                "_sdc_source_lineno": 1
            },
            "mytable_list": {
                "int": 1,
                "list_int": [1, 2, 3],
                "list_float": [Decimal(1.2), Decimal(2.3), Decimal(3.4)],
                "list_string": ["data", "of", "string"],
                "list_dict": [{"id": 1}, {"id": 2}],
                "list_list": ["[1, 2, 3]", "[4, 5, 6]"],
                "list_datetime": ["2018-01-01T19:29:14.578000Z", "2019-01-01T19:29:14.578000Z"],
                "_sdc_source_lineno": 1
            },
            "mytable_dict": {
                "int": 1,
                "dict_int": {"1": 1, "2": 2},
                "dict_float": {"1.2": Decimal(2.0)},
                "dict_string": {"key1": "value1", "key2": "value2"},
                "dict_dict": {"key": {"name": "john"}},
                "dict_list": {"ids": [1, 2, 3]},
                "dict_datetime": {"date_1": "2018-01-01T19:29:14.578000Z", "date_2": "2019-01-01T19:29:14.578000Z"},
                "_sdc_source_lineno": 1
            }
        }

    def test_run(self):
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
        self.assertEqual(self.expected_check_streams(), found_catalog_names)

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
            connections.select_catalog_and_fields_via_metadata(conn_id,
                                                               stream_catalog,
                                                               annotated_schema['annotated-schema'],
                                                               [])

        # Run sync
        sync_job_name = runner.run_sync_mode(self, conn_id)

        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # verify the persisted schema was correct
        messages_by_stream = runner.get_records_from_target_output()

        for stream in self.expected_check_streams():
            records = [record.get("data") for record in messages_by_stream.get(stream, {}).get("messages", [])
                       if record.get("action") == "upsert"]
            self.assertEqual(len(records), 1)
            for record in records:
                del record["_sdc_source_file"]
            self.assertEqual([self.expected_data().get(stream)], records)
