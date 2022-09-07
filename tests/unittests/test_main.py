import unittest
from unittest import mock
from tap_sftp import main, stream_is_selected
from parameterized import parameterized
import paramiko

class ParsedArgs:
    """
        Class to provide mocked parsed arguments.
    """

    def __init__(self,discover=False,config = None,state = None, catalog = None) -> None:
        self.discover = discover
        self.config = config
        self.state = state
        self.catalog = catalog

class TestSFTPInit(unittest.TestCase):
    '''
        Test class to verify proper working of functions in __init__.py file.
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

    @parameterized.expand([
        ["discover_called", [True,False], [True, False]],
        ["sync_called", [False,True], [False, True]],
    ])
    @mock.patch('singer.utils.parse_args')
    @mock.patch('tap_sftp.do_discover')
    @mock.patch('tap_sftp.do_sync')
    def test_init(self, test_name, test_data, exp, mock_sync, mock_discover, mock_args):
        """
            Test case to verify working of  init file for different flag scenarios.
        """

        # Return mocked args
        mock_args.return_value = ParsedArgs(discover = test_data[0],
                                catalog = test_data[1], config=self.config)
        main()

        self.assertEqual(mock_discover.called,exp[0])
        self.assertEqual(mock_sync.called,exp[1])

    @parameterized.expand([
      ['stream_selected',True,True],
      ['stream_not_selected',False,False],
    ])
    @mock.patch("tap_sftp.client.SFTPConnection.sftp")
    def test_stream_selection(self, test_name, actual_value, expected_value, mocked_connect):
        """
            Test Stream is selected/not-selected
        """

        mocked_connect.side_effect = paramiko.SFTPClient
        metadata = {(): {'selected':actual_value }}
        stream_is_selected(metadata)
        self.assertEqual(actual_value, expected_value)
