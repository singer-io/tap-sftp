import unittest
from unittest import mock
import gzip
import tap_sftp.client as client
import paramiko

@mock.patch("tap_sftp.client.SFTPConnection.sftp")
@mock.patch("tap_sftp.client.LOGGER.info")
class TestEmptyCSVinGZ(unittest.TestCase):

    @mock.patch("gzip.GzipFile")
    def test_empty_file_negative(self, mocked_gzip, mocked_logger, mocked_connect):

        mocked_connect.return_value = paramiko.SFTPClient
        mocked_gzip.side_effect = mock.mock_open(read_data='')

        conn = client.SFTPConnection("10.0.0.1", "username", port="22")

        gzip = conn.check_gzip_to_skip("/root_dir/file.csv.gz")

        self.assertEquals(gzip, True)
        mocked_logger.assert_called_with("Skipping %s file as it is empty.", "/root_dir/file.csv.gz")

    @mock.patch("gzip.GzipFile")
    def test_empty_file_positive(self, mocked_gzip, mocked_logger, mocked_connect):

        mocked_connect.return_value = paramiko.SFTPClient
        mocked_gzip.side_effect = mock.mock_open(read_data='a')

        conn = client.SFTPConnection("10.0.0.1", "username", port="22")

        gzip = conn.check_gzip_to_skip("/root_dir/file.csv.gz")

        self.assertEquals(gzip, False)

    @mock.patch("gzip.GzipFile.read")
    def test_empty_file_OSError(self, mocked_gzip, mocked_logger, mocked_connect):

        mocked_connect.return_value = paramiko.SFTPClient
        mocked_gzip.side_effect = OSError
        conn = client.SFTPConnection("10.0.0.1", "username", port="22")

        gzip = conn.check_gzip_to_skip("/root_dir/file.csv.gz")

        self.assertEquals(gzip, True)
        mocked_logger.assert_called_with("Skipping %s file as it is not a gzipped file.", "/root_dir/file.csv.gz")

    @mock.patch("gzip.GzipFile.read")
    def test_empty_file_PermissionDenied(self, mocked_gzip, mocked_logger, mocked_connect):

        mocked_connect.return_value = paramiko.SFTPClient
        mocked_gzip.side_effect = PermissionError("Permission denied")
        conn = client.SFTPConnection("10.0.0.1", "username", port="22")

        gzip = conn.check_gzip_to_skip("/root_dir/file.csv.gz")

        self.assertEquals(gzip, True)
        mocked_logger.assert_called_with("Skipping %s file as you do not have enough permissions.", "/root_dir/file.csv.gz")
