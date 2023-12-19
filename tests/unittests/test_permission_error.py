import unittest
from unittest import mock
import tap_sftp.client as client
import tap_sftp.sync as sync
import paramiko

DEFAULT_ENCODING_FORMAT = "utf-8"
@mock.patch("tap_sftp.client.SFTPConnection.sftp")
@mock.patch("tap_sftp.client.LOGGER.warn")
class TestPermissionError(unittest.TestCase):

    def test_no_error(self, mocked_logger, mocked_connect):

        mocked_connect.side_effect = paramiko.SFTPClient
        mocked_connect.open.side_effect = mock.mock_open()

        conn = client.SFTPConnection("10.0.0.1", "username", port="22")

        file_handle = conn.get_file_handle({"filepath": "/root_dir/file.csv.gz", "last_modified": "2020-01-01"})
        self.assertEquals(0, mocked_logger.call_count)

    def test_file_opening_error(self, mocked_logger, mocked_connect):

        mocked_connect.side_effect = paramiko.SFTPClient
        mocked_connect.open.side_effect = OSError

        conn = client.SFTPConnection("10.0.0.1", "username", port="22")

        try:
            file_handle = conn.get_file_handle({"filepath": "/root_dir/file.csv.gz", "last_modified": "2020-01-01"})
        except OSError:
            # check if logger is called if logger is called in the function 
            # then error has occurred otherwise not
            mocked_logger.assert_called_with("Skipping %s file because it is unable to be read.", "/root_dir/file.csv.gz")

    def test_permission_error(self, mocked_logger, mocked_connect):

        mocked_connect.side_effect = paramiko.SFTPClient
        mocked_connect.open.side_effect = PermissionError("Permission denied")

        conn = client.SFTPConnection("10.0.0.1", "username", port="22")

        try:
            file_handle = conn.get_file_handle({"filepath": "/root_dir/file.csv.gz", "last_modified": "2020-01-01"})
        except OSError:
            # check if logger is called if logger is called in the function 
            # then error has occurred otherwise not
            mocked_logger.assert_called_with("Skipping %s file because you do not have enough permissions.", "/root_dir/file.csv.gz")

    @mock.patch("tap_sftp.stats.add_file_data")
    @mock.patch("singer_encodings.csv.get_row_iterators")
    def test_no_error_during_sync(self, mocked_get_row_iterators, mocked_stats, mocked_logger, mocked_connect):
        mocked_connect.side_effect = paramiko.SFTPClient
        mocked_get_row_iterators.return_value = []
        mocked_connect.open.side_effect = mock.mock_open()

        conn = client.SFTPConnection("10.0.0.1", "username", port="22")

        rows_synced = sync.sync_file(conn, 
                                     {"filepath": "/root_dir/file.csv.gz", "last_modified": "2020-01-01"}, 
                                     None, 
                                     {"key_properties": ["id"], "delimiter": ","}, 
                                     encoding_format=DEFAULT_ENCODING_FORMAT)
        # check if "csv.get_row_iterators" is called if it is called then error has not occurred
        # if it is not called then error has occured and function returned from the except block
        self.assertEquals(1, mocked_get_row_iterators.call_count)

    @mock.patch("singer_encodings.csv.get_row_iterators")
    def test_permisison_error_during_sync(self, mocked_get_row_iterators, mocked_logger, mocked_connect):
        mocked_connect.side_effect = paramiko.SFTPClient
        mocked_get_row_iterators.return_value = []
        mocked_connect.open.side_effect = PermissionError("Permission denied")

        conn = client.SFTPConnection("10.0.0.1", "username", port="22")

        rows_synced = sync.sync_file(conn, 
                                     {"filepath": "/root_dir/file.csv.gz", "last_modified": "2020-01-01"}, 
                                     None,
                                     {"key_properties": ["id"], "delimiter": ","}, 
                                     encoding_format=DEFAULT_ENCODING_FORMAT)
        # check if "csv.get_row_iterators" is called if it is called then error has not occurred
        # if it is not called then error has occured and function returned from the except block
        self.assertEquals(0, mocked_get_row_iterators.call_count)
        mocked_logger.assert_called_with("Skipping %s file because you do not have enough permissions.", "/root_dir/file.csv.gz")

    @mock.patch("singer_encodings.csv.get_row_iterators")
    def test_oserror_during_sync(self, mocked_get_row_iterators, mocked_logger, mocked_connect):
        mocked_connect.side_effect = paramiko.SFTPClient
        mocked_get_row_iterators.return_value = []
        mocked_connect.open.side_effect = OSError

        conn = client.SFTPConnection("10.0.0.1", "username", port="22")

        rows_synced = sync.sync_file(conn, 
                                     {"filepath": "/root_dir/file.csv.gz", "last_modified": "2020-01-01"}, 
                                     None, 
                                     {"key_properties": ["id"], "delimiter": ","}, 
                                     encoding_format=DEFAULT_ENCODING_FORMAT)
        # check if "csv.get_row_iterators" is called if it is called then error has not occurred
        # if it is not called then error has occured and function returned from the except block
        self.assertEquals(0, mocked_get_row_iterators.call_count)
        mocked_logger.assert_called_with("Skipping %s file because it is unable to be read.", "/root_dir/file.csv.gz")
