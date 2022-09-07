import unittest
from unittest import mock
from unittest.mock import call
from tap_sftp import do_discover
import paramiko

@mock.patch("tap_sftp.client.SFTPConnection.sftp")
class TestDiscover(unittest.TestCase):
    '''
        Test class to verify proper working of discover mode functions.
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

    @mock.patch("tap_sftp.LOGGER.info")
    def test_do_discover(self,mocked_logger, mocked_connect):
        """
            Test case to verify that discover function executes successfully.
        """

        mocked_connect.side_effect = paramiko.SFTPClient
        calls = [
            call('Starting discover'),
            call('Sampling records to determine table JSON schema "%s".','data'),
            call('Finished discover')
        ]

        do_discover(self.config)

        self.assertEqual(mocked_logger.call_count,3)
        mocked_logger.assert_has_calls(calls, any_order= True)

    @mock.patch('tap_sftp.discover_streams')
    def test_do_discover_raises_error(self, mocked_discover, mocked_connect):
        """
            Test case to verify that error is raised when no streams are discovered.
        """

        mocked_connect.side_effect = paramiko.SFTPClient
        mocked_discover.return_value = False

        with self.assertRaises(Exception) as e:
            do_discover(self.config)

        self.assertEqual(str(e.exception), "No streams found")
