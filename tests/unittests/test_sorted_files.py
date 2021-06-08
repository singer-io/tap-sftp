from time import time
import unittest
from unittest import mock
import tap_sftp.client as client
from datetime import datetime
import time
import pytz

@mock.patch("tap_sftp.client.SFTPConnection.get_files_by_prefix")
@mock.patch("tap_sftp.client.SFTPConnection.get_files_matching_pattern")
class TestSortedFiles(unittest.TestCase):

    def test_sorted_files(self, mocked_matching_files, mocked_all_files):
        conn = client.SFTPConnection("10.0.0.1", "username", port="22")

        files_list = [
            {
                "filepath": "/root/file1.csv",
                "last_modified": datetime.utcfromtimestamp(time.time() - 10).replace(tzinfo=pytz.UTC)
            },
            {
                "filepath": "/root/file2.csv",
                "last_modified": datetime.utcfromtimestamp(time.time() - 4).replace(tzinfo=pytz.UTC)
            },
            {
                "filepath": "/root/file3.csv",
                "last_modified": datetime.utcfromtimestamp(time.time() - 8).replace(tzinfo=pytz.UTC)
            },
            {
                "filepath": "/root/file4.csv",
                "last_modified": datetime.utcfromtimestamp(time.time()).replace(tzinfo=pytz.UTC)
            },
            {
                "filepath": "/root/file.txt",
                "last_modified": datetime.utcfromtimestamp(time.time() - 2).replace(tzinfo=pytz.UTC)
            },
            {
                "filepath": "/root/file5.json",
                "last_modified": datetime.utcfromtimestamp(time.time() - 3).replace(tzinfo=pytz.UTC)
            }]

        mocked_all_files.return_value = files_list

        mocked_matching_files.return_value = files_list[:4]

        files = conn.get_files("/root", "file.*.csv")

        # expected files in decreasing order of "last_modified"
        expected_files_list = ["/root/file4.csv", "/root/file2.csv", "/root/file3.csv", "/root/file1.csv"]
        acutal_files_list = [f["filepath"] for f in files]

        self.assertEquals(expected_files_list, acutal_files_list)
