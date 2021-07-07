import unittest
from unittest import mock
import gzip

from paramiko import file
import tap_sftp.client as client
import tap_sftp.sync as sync
import paramiko

@mock.patch("tap_sftp.client.SFTPConnection.sftp")
@mock.patch("tap_sftp.client.LOGGER.info")
class TestPermissionError(unittest.TestCase):

    def test_no_error(self, mocked_logger, mocked_connect):

        mocked_connect.side_effect = paramiko.SFTPClient
        mocked_connect.open.side_effect = mock.mock_open()

        conn = client.SFTPConnection("10.0.0.1", "username", port="22")

        file_handle = conn.get_file_handle({"filepath": "/root_dir/file.csv.gz", "last_modified": "2020-01-01"})
        self.assertEquals(0, mocked_logger.call_count)
        self.assertTrue(file_handle is not None)

    def test_file_opening_error(self, mocked_logger, mocked_connect):

        mocked_connect.side_effect = paramiko.SFTPClient
        mocked_connect.open.side_effect = OSError

        conn = client.SFTPConnection("10.0.0.1", "username", port="22")

        file_handle = conn.get_file_handle({"filepath": "/root_dir/file.csv.gz", "last_modified": "2020-01-01"})
        self.assertEquals(None, file_handle)
        mocked_logger.assert_called_with("Skipping %s file as there is some problem in opening it.", "/root_dir/file.csv.gz")

    def test_permission_error(self, mocked_logger, mocked_connect):

        mocked_connect.side_effect = paramiko.SFTPClient
        mocked_connect.open.side_effect = PermissionError("Permission denied")

        conn = client.SFTPConnection("10.0.0.1", "username", port="22")

        file_handle = conn.get_file_handle({"filepath": "/root_dir/file.csv.gz", "last_modified": "2020-01-01"})
        self.assertEquals(None, file_handle)
        mocked_logger.assert_called_with("Skipping %s file as you do not have enough permissions.", "/root_dir/file.csv.gz")

    @mock.patch("tap_sftp.stats.add_file_data")
    @mock.patch("singer_encodings.csv.get_row_iterators")
    def test_no_during_sync(self, mocked_get_row_iterators, mocked_stats, mocked_logger, mocked_connect):
        mocked_connect.side_effect = paramiko.SFTPClient
        mocked_get_row_iterators.return_value = []
        mocked_connect.open.side_effect = mock.mock_open()

        conn = client.SFTPConnection("10.0.0.1", "username", port="22")

        rows_synced = sync.sync_file(conn, {"filepath": "/root_dir/file.csv.gz", "last_modified": "2020-01-01"}, None, {"key_properties": ["id"], "delimiter": ","})
        self.assertEquals(0, rows_synced)
        self.assertEquals(1, mocked_get_row_iterators.call_count)

    @mock.patch("singer_encodings.csv.get_row_iterators")
    def test_permisison_error_during_sync(self, mocked_get_row_iterators, mocked_logger, mocked_connect):
        mocked_connect.side_effect = paramiko.SFTPClient
        mocked_get_row_iterators.return_value = []
        mocked_connect.open.side_effect = PermissionError("Permission denied")

        conn = client.SFTPConnection("10.0.0.1", "username", port="22")

        rows_synced = sync.sync_file(conn, {"filepath": "/root_dir/file.csv.gz", "last_modified": "2020-01-01"}, None, {"key_properties": ["id"], "delimiter": ","})
        self.assertEquals(0, rows_synced)
        self.assertEquals(0, mocked_get_row_iterators.call_count)
        mocked_logger.assert_called_with("Skipping %s file as you do not have enough permissions.", "/root_dir/file.csv.gz")

    @mock.patch("singer_encodings.csv.get_row_iterators")
    def test_oserror_during_sync(self, mocked_get_row_iterators, mocked_logger, mocked_connect):
        mocked_connect.side_effect = paramiko.SFTPClient
        mocked_get_row_iterators.return_value = []
        mocked_connect.open.side_effect = OSError

        conn = client.SFTPConnection("10.0.0.1", "username", port="22")

        rows_synced = sync.sync_file(conn, {"filepath": "/root_dir/file.csv.gz", "last_modified": "2020-01-01"}, None, {"key_properties": ["id"], "delimiter": ","})
        self.assertEquals(0, rows_synced)
        self.assertEquals(0, mocked_get_row_iterators.call_count)
        mocked_logger.assert_called_with("Skipping %s file as there is some problem in opening it.", "/root_dir/file.csv.gz")
