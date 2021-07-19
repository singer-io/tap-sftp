import unittest
from unittest import mock
import zipfile
import tap_sftp.client as client
import paramiko

class ZipFile:
    data = None
    def __init__(self, data):
        self.data = data

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        pass

    def namelist(self):
        return ["test.csv"]
    
    def open(self, name):
        return self

    def read(self):
        return self.data

@mock.patch("tap_sftp.client.SFTPConnection.sftp")
@mock.patch("zipfile.ZipFile")
@mock.patch("tap_sftp.client.LOGGER.warn")
class TestEmptyCSVinZIP(unittest.TestCase):

    def test_empty_zip_file(self, mocked_logger, mocked_zip, mocked_connect):

        mocked_connect.return_value = paramiko.SFTPClient
        mocked_zip.return_value = ZipFile("")
        conn = client.SFTPConnection("10.0.0.1", "username", port="22")

        gzip = conn.should_skip_zip_file("/root_dir/file.zip")

        self.assertEquals(gzip, True)
        mocked_logger.assert_called_with("Skipping %s file because it is empty.", "/root_dir/file.zip/test.csv")

    def test_not_empty_zip_file(self, mocked_logger, mocked_zip, mocked_connect):

        mocked_connect.return_value = paramiko.SFTPClient
        mocked_zip.return_value = ZipFile("data")
        conn = client.SFTPConnection("10.0.0.1", "username", port="22")

        gzip = conn.should_skip_zip_file("/root_dir/file.zip")

        self.assertEquals(gzip, False)
        self.assertEquals(mocked_logger.call_count, 0)

    def test_zip_OSError(self, mocked_logger, mocked_zip, mocked_connect):

        mocked_connect.return_value = paramiko.SFTPClient
        mocked_zip.side_effect = OSError
        conn = client.SFTPConnection("10.0.0.1", "username", port="22")

        gzip = conn.should_skip_zip_file("/root_dir/file.zip")

        self.assertEquals(gzip, True)
        mocked_logger.assert_called_with("Skipping %s file because it is not a zipped file.", "/root_dir/file.zip")

    def test_zip_PermissionError(self, mocked_logger, mocked_zip, mocked_connect):

        mocked_connect.return_value = paramiko.SFTPClient
        mocked_zip.side_effect = PermissionError("Permission denied")
        conn = client.SFTPConnection("10.0.0.1", "username", port="22")

        gzip = conn.should_skip_zip_file("/root_dir/file.zip")

        self.assertEquals(gzip, True)
        mocked_logger.assert_called_with("Skipping %s file because you do not have enough permissions.", "/root_dir/file.zip")

    def test_zip_BadZipFileError(self, mocked_logger, mocked_zip, mocked_connect):

        mocked_connect.return_value = paramiko.SFTPClient
        mocked_zip.side_effect = zipfile.BadZipFile
        conn = client.SFTPConnection("10.0.0.1", "username", port="22")

        gzip = conn.should_skip_zip_file("/root_dir/file.zip")

        self.assertEquals(gzip, True)
        mocked_logger.assert_called_with("Skipping %s file because it is not a zipped file.", "/root_dir/file.zip")
