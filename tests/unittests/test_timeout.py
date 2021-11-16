import socket
import unittest
from unittest import mock
from tap_sftp import client, discover, sync

class TestTimeoutValue(unittest.TestCase):
    """
        Test case to verify the timeout value is set as expected
    """

    def test_timeout_value_not_passed_in_config(self):
        # create config
        config = {
            "host":"10.0.0.1",
            "port":22,
            "username":"username",
            "password": "",
            "start_date":"2020-01-01"
        }

        # create connection
        conn = client.connection(config=config)  

        # verify the expected timeout value is set
        self.assertEquals(conn.request_timeout, 300)

    def test_timeout_int_value_passed_in_config(self):
        # create config
        config = {
            "host":"10.0.0.1",
            "port":22,
            "username":"username",
            "password": "",
            "start_date":"2020-01-01",
            "request_timeout": 100
        }

        # create connection
        conn = client.connection(config=config)  

        # verify the expected timeout value is set
        self.assertEquals(conn.request_timeout, 100.0)

    def test_timeout_string_value_passed_in_config(self):
        # create config
        config = {
            "host":"10.0.0.1",
            "port":22,
            "username":"username",
            "password": "",
            "start_date":"2020-01-01",
            "request_timeout": "100"
        }

        # create connection
        conn = client.connection(config=config)  

        # verify the expected timeout value is set
        self.assertEquals(conn.request_timeout, 100.0)

    def test_timeout_empty_value_passed_in_config(self):
        # create config
        config = {
            "host":"10.0.0.1",
            "port":22,
            "username":"username",
            "password": "",
            "start_date":"2020-01-01",
            "request_timeout": ""
        }

        # create connection
        conn = client.connection(config=config)  

        # verify the expected timeout value is set
        self.assertEquals(conn.request_timeout, 300)

    def test_timeout_0_value_passed_in_config(self):
        # create config
        config = {
            "host":"10.0.0.1",
            "port":22,
            "username":"username",
            "password": "",
            "start_date":"2020-01-01",
            "request_timeout": 0.0
        }

        # create connection
        conn = client.connection(config=config)  

        # verify the expected timeout value is set
        self.assertEquals(conn.request_timeout, 300)

    def test_timeout_string_0_value_passed_in_config(self):
        # create config
        config = {
            "host":"10.0.0.1",
            "port":22,
            "username":"username",
            "password": "",
            "start_date":"2020-01-01",
            "request_timeout": "0.0"
        }

        # create connection
        conn = client.connection(config=config)  

        # verify the expected timeout value is set
        self.assertEquals(conn.request_timeout, 300)

class TimeoutBackoff(unittest.TestCase):
    """
        Test case to verify the tap back off for 5 times for 'socket.timeout' error
    """

    @mock.patch("time.sleep")
    @mock.patch("singer.metadata.get_standard_metadata")
    @mock.patch("singer_encodings.json_schema.get_schema_for_table")
    def test_timeout_backoff__get_schema(self, mocked_get_schema_for_table, mocked_get_standard_metadata, mocked_sleep):
        # mock 'get_schema_for_table' and raise 'socket.timeout' error
        mocked_get_schema_for_table.side_effect = socket.timeout

        table_spec = {
            "table_name": "test"
        }
        try:
            # function call
            discover.get_schema("test_conn", table_spec)
        except socket.timeout:
            pass

        # verify that the tap backoff for 5 times
        self.assertEquals(mocked_get_schema_for_table.call_count, 5)

    @mock.patch("time.sleep")
    @mock.patch("tap_sftp.client.SFTPConnection.get_file_handle")
    @mock.patch("singer_encodings.csv.get_row_iterators")
    def test_timeout_backoff__sync_file(self, mocked_get_row_iterators, mocked_get_file_handle, mocked_sleep):

        # mock 'get_row_iterators' and raise 'socket.timeout' error
        mocked_get_row_iterators.side_effect = socket.timeout
        # mock 'get_file_handle'
        mocked_get_file_handle.return_value = None

        config = {
            "host":"10.0.0.1",
            "port":22,
            "username":"username",
            "password": "",
            "start_date":"2020-01-01"
        }
        table_spec = {
            "key_properties": "test_key_properties",
            "delimiter": ","
        }
        file = {
            "filepath": "/root/file.csv"
        }
        # create connection
        conn = client.connection(config=config)
        try:
            # function call
            sync.sync_file(conn=conn, f=file, stream="test_stream", table_spec=table_spec)
        except socket.timeout:
            pass

        # verify that the tap backoff for 5 times
        self.assertEquals(mocked_get_row_iterators.call_count, 5)

    @mock.patch("time.sleep")
    @mock.patch("tap_sftp.client.SFTPConnection.sftp")
    def test_timeout_backoff__get_files_by_prefix(self, mocked_sftp, mocked_sleep):

        # mock 'listdir_attr' and raise 'socket.timeout' error
        mocked_listdir_attr = mock.Mock()
        mocked_listdir_attr.side_effect = socket.timeout
        mocked_sftp.listdir_attr.side_effect = mocked_listdir_attr

        config = {
            "host":"10.0.0.1",
            "port":22,
            "username":"username",
            "password": "",
            "start_date":"2020-01-01"
        }
        # create connection
        conn = client.connection(config=config)
        try:
            # function call
            conn.get_files_by_prefix(".")
        except socket.timeout:
            pass

        # verify that the tap backoff for 5 times
        self.assertEquals(mocked_sftp.listdir_attr.call_count, 5)
