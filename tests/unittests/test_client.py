import unittest
from unittest import mock
from tap_sftp.client import SFTPConnection

class TestSFTPClient(unittest.TestCase):
    '''
        Test class to verify proper working of functions in client.py file.
    '''

    config = {
            "host":"10.0.0.1",
            "port":22,
            "username":"username",
            "password": "",
            "start_date":"2020-01-01",
            "tables":"[\
                {\
                    \"table_name\":\"data\",\
                    \"search_prefix\":\"\",\
                    \"search_pattern\":\"txt\",\
                    \"key_properties\":[],\
                    \"delimiter\":\",\",\
                    \"date_overrides\":\"date\"\
                }\
            ]"
    }

    @mock.patch("paramiko.sftp_attr.SFTPAttributes")
    @mock.patch("tap_sftp.client.SFTPConnection.sftp")
    def test_get_files_by_prefix(self, mocked_connection, mocked_sftp_attr):
        '''
            Test class to verify the get_files_by_prefix function works correctly.
        '''

        connect = SFTPConnection(self.config['host'],
                          self.config['username'],
                          password=self.config.get('password'),
                          port=self.config.get('port'),
                          )
        mocked_connection.listdir_attr.return_value = [mocked_sftp_attr]
        mocked_sftp_attr.st_size = 1
        mocked_sftp_attr.st_mode = 33188
        mocked_sftp_attr.st_mtime = None

        files = connect.get_files_by_prefix("/test")

        self.assertEqual(type(files), list)

    @mock.patch("tap_sftp.client.SFTPConnection.sftp")
    def test_get_files_by_prefix_raises_error(self, mocked_connection):
        '''
            Test case to verify that error is raised correctly when incorrect
            file or directory is provided.
        '''

        connect = SFTPConnection(self.config['host'],
                          self.config['username'],
                          password=self.config.get('password'),
                          port=self.config.get('port'),
                          )
        mocked_connection.listdir_attr.side_effect = FileNotFoundError

        with self.assertRaises(Exception) as e:
            connect.get_files_by_prefix("/test")

        self.assertEqual(str(e.exception), "Directory '/test' does not exist")

    @mock.patch("paramiko.RSAKey.from_private_key_file")
    def test_set_key_by_private_key_file(self, mocked_private_file):
        '''
            Test case to verify that when private_key_file is provided then the "key"
            attribute is set accordingly.
        '''
        mocked_private_file.return_value = "test_key"
        connect = SFTPConnection(self.config['host'],
                          self.config['username'],
                          password=self.config.get('password'),
                          port=self.config.get('port'),
                          private_key_file='/test'
                          )

        self.assertEqual(connect.key, "test_key")
