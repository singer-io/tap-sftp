from time import time
import unittest
from unittest import mock
import tap_sftp.client as client
from datetime import datetime
import time
import pytz

@mock.patch("tap_sftp.client.SFTPConnection.get_files_by_prefix")
@mock.patch("tap_sftp.client.SFTPConnection.should_skip_zip_file")
@mock.patch("tap_sftp.client.SFTPConnection.should_skip_gzip_file")
class TestEmptyGZAndZIP(unittest.TestCase):
    files_list = [
            {
                "filepath": "/root/file1.zip",
                "last_modified": datetime.utcfromtimestamp(time.time() - 10).replace(tzinfo=pytz.UTC)
            },
            {
                "filepath": "/root/file2.csv",
                "last_modified": datetime.utcfromtimestamp(time.time() - 4).replace(tzinfo=pytz.UTC)
            },
            {
                "filepath": "/root/file3.csv.gz",
                "last_modified": datetime.utcfromtimestamp(time.time() - 8).replace(tzinfo=pytz.UTC)
            },
            {
                "filepath": "/root/file4.zip",
                "last_modified": datetime.utcfromtimestamp(time.time()).replace(tzinfo=pytz.UTC)
            },
            {
                "filepath": "/root/file.txt",
                "last_modified": datetime.utcfromtimestamp(time.time() - 2).replace(tzinfo=pytz.UTC)
            },
            {
                "filepath": "/root/file5.csv.gz",
                "last_modified": datetime.utcfromtimestamp(time.time() - 3).replace(tzinfo=pytz.UTC)
            }]

    def test_empty_zip(self, mocked_skip_gzip_files, mocked_skip_zip_files, mocked_all_files):
        conn = client.SFTPConnection("10.0.0.1", "username", port="22")

        mocked_all_files.return_value = self.files_list
        mocked_skip_zip_files.return_value = True
        mocked_skip_gzip_files.return_value = False

        files = conn.get_files("/root", "")

        expected_files_list = ["/root/file3.csv.gz", "/root/file2.csv", "/root/file5.csv.gz", "/root/file.txt"]
        actual_files_list = [f["filepath"] for f in files]

        self.assertEquals(expected_files_list, actual_files_list)

    def test_empty_gz(self, mocked_skip_gzip_files, mocked_skip_zip_files, mocked_all_files):
        conn = client.SFTPConnection("10.0.0.1", "username", port="22")

        mocked_all_files.return_value = self.files_list
        mocked_skip_zip_files.return_value = False
        mocked_skip_gzip_files.return_value = True

        files = conn.get_files("/root", "")

        expected_files_list = ["/root/file1.zip", "/root/file2.csv", "/root/file.txt", "/root/file4.zip"]
        actual_files_list = [f["filepath"] for f in files]

        self.assertEquals(expected_files_list, actual_files_list)

    def test_no_empty_file(self, mocked_skip_gzip_files, mocked_skip_zip_files, mocked_all_files):
        conn = client.SFTPConnection("10.0.0.1", "username", port="22")

        mocked_all_files.return_value = self.files_list
        mocked_skip_zip_files.return_value = False
        mocked_skip_gzip_files.return_value = False

        files = conn.get_files("/root", "")

        expected_files_list = ["/root/file1.zip", "/root/file3.csv.gz", "/root/file2.csv", "/root/file5.csv.gz", "/root/file.txt", "/root/file4.zip"]
        actual_files_list = [f["filepath"] for f in files]

        self.assertEquals(expected_files_list, actual_files_list)
