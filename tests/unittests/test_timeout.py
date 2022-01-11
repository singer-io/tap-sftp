from datetime import datetime
import socket
import unittest
from unittest import mock
from tap_sftp import client, discover, sync

class TestTimeoutValue(unittest.TestCase):
    """
        Test cases to verify the timeout value is set as expected
    """

    def test_timeout_value_not_passed_in_config(self):
        """
            Test case to verify that the timeout value is 300 as
            we have not passed 'request_timeout' in config
        """
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
        """
            Test case to verify that the timeout value is 100 as we
            have passed 'request_timeout' in integer format in config
        """
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
        """
            Test case to verify that the timeout value is 100 as we
            have passed 'request_timeout' in string format in config
        """
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
        """
            Test case to verify that the timeout value is 300 as we
            have passed empty value in 'request_timeout' in config
        """
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
        """
            Test case to verify that the timeout value is 300 as we
            have passed 0 value in 'request_timeout' in config
        """
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
        """
            Test case to verify that the timeout value is 300 as we
            have passed string 0 value in 'request_timeout' in config
        """
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
        Test cases to verify the tap back off for 5 times for 'socket.timeout' error
    """

    @mock.patch("singer.metadata.get_standard_metadata")
    @mock.patch("singer_encodings.json_schema.get_schema_for_table")
    def test_timeout_backoff__get_schema(self, mocked_get_schema_for_table, mocked_get_standard_metadata):
        """
            Test case to verify we backoff and retry for 'get_schema' function
        """
        # mock 'get_schema_for_table' and raise 'socket.timeout' error
        mocked_get_schema_for_table.side_effect = socket.timeout

        table_spec = {
            "table_name": "test"
        }
        before_time = datetime.now()
        with self.assertRaises(socket.timeout):
            # function call
            discover.get_schema("test_conn", table_spec)
        after_time = datetime.now()

        # verify that the tap backoff for 60 seconds
        time_difference = (after_time - before_time).total_seconds()
        self.assertGreaterEqual(time_difference, 60)

    @mock.patch("time.sleep")
    @mock.patch("tap_sftp.client.SFTPConnection.get_file_handle")
    @mock.patch("singer_encodings.csv.get_row_iterators")
    def test_timeout_backoff__sync_file(self, mocked_get_row_iterators, mocked_get_file_handle, mocked_sleep):
        """
            Test case to verify we backoff and retry for 'sync_file' function
        """
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
        with self.assertRaises(socket.timeout):
            # function call
            sync.sync_file(conn=conn, f=file, stream="test_stream", table_spec=table_spec)

        # verify that the tap backoff for 5 times
        self.assertEquals(mocked_get_row_iterators.call_count, 5)

    @mock.patch("tap_sftp.client.SFTPConnection.sftp")
    def test_timeout_backoff__get_files_by_prefix(self, mocked_sftp):
        """
            Test case to verify we backoff and retry for 'get_files_by_prefix' function
        """
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

        before_time = datetime.now()
        with self.assertRaises(socket.timeout):
            # function call
            conn.get_files_by_prefix(".")
        after_time = datetime.now()

        # verify that the tap backoff for 60 seconds
        time_difference = (after_time - before_time).total_seconds()
        self.assertGreaterEqual(time_difference, 60)

    @mock.patch("time.sleep")
    @mock.patch("tap_sftp.client.SFTPConnection.sftp")
    def test_timeout_backoff__get_file_handle(self, mocked_sftp, mocked_sleep):
        """
            Test case to verify we backoff and retry for 'get_file_handle' function
        """
        # mock 'open' and raise 'socket.timeout' error
        mocked_open = mock.Mock()
        mocked_open.side_effect = socket.timeout
        mocked_sftp.open.side_effect = mocked_open

        config = {
            "host":"10.0.0.1",
            "port":22,
            "username":"username",
            "password": "",
            "start_date":"2020-01-01"
        }
        # create connection
        conn = client.connection(config=config)

        with self.assertRaises(socket.timeout):
            # function call
            conn.get_file_handle({"filepath": "/root/file.csv"})

        # verify that the tap backoff for 5 times
        self.assertEquals(mocked_sftp.open.call_count, 5)
