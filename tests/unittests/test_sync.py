import unittest
from unittest import mock
from unittest.mock import call
import datetime
from parameterized import parameterized
import paramiko
from tap_sftp.sync import sync_file, sync_stream
from tap_sftp import do_sync, stats

class Schema:
    '''
        Class to provide schema in dictionary format for given stream.
    '''
    def __init__(self, stream):
        self.stream = stream

    def to_dict(self):
        '''
            Providing a dictionary of the schema.
        '''
        return {'streams': self.stream}

class Stream:
    '''
        Class to provide required attributes for streams.
    '''
    def __init__(self, stream_name):
        self.tap_stream_id = stream_name
        self.stream = stream_name
        self.schema = Schema(stream_name)
        self.metadata = {}

class Catalog:
    '''
        Class to provide catalog for given streams.
    '''
    def __init__(self, streams):
        self.streams = [Stream(streams)]

class TestSync(unittest.TestCase):
    '''
        Test class to verify proper working of functions in mode.
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
      ['stream_selected',True],
      ['stream_not_selected',False]])
    @mock.patch('tap_sftp.stream_is_selected')
    @mock.patch("tap_sftp.client.SFTPConnection.sftp")
    @mock.patch("singer.write_schema")
    @mock.patch("tap_sftp.LOGGER")
    def test_do_sync(self,test_name, expected_value, mocked_logger,
                     mocked_schema, mocked_connect, mocked_select):
        """
            Test case to verify that sync mode executes successfully.
        """

        mocked_connect.side_effect = paramiko.SFTPClient
        mocked_select.return_value = expected_value

        catalog=Catalog('data')
        table_spec = {'table_name': 'data', 'search_prefix': '', 'search_pattern': 'txt'}

        # Setting the GLOBAL variable STATS
        stats.add_file_data(table_spec, '/test.txt', datetime.datetime.now(), 0)
        do_sync(self.config,catalog,state={'start_date': '2020-01-01'})

        self.assertEqual(mocked_select.call_count,1)
        mocked_logger.info.assert_called_with('Done syncing.')

    @mock.patch("tap_sftp.sync.LOGGER.info")
    def test_sync_stream_no_table_spec(self,mocked_logger):
        '''
            Test case to verify that a stream is skipped if table_spec is
            not provided for it.
        '''
        calls = [
            call('Syncing table "%s".', 'test'),
            call(
                'Getting files modified since %s.',
                datetime.datetime(2020, 1, 1, 0, 0, tzinfo=datetime.timezone.utc)
                ),
            call("No table configuration found for '%s', skipping stream", 'test')]

        sync_stream(self.config, state={'start_date': '2020-01-01'}, stream = Stream("test"))

        self.assertEqual(mocked_logger.call_count,3)
        mocked_logger.assert_has_calls(calls, any_order= True)

    @mock.patch("tap_sftp.sync.LOGGER.info")
    def test_sync_stream_multiple_table_spec(self, mocked_logger):
        '''
            Test case to verify that a stream is skipped more than one table_spec
            are provided for it.
        '''
        test_config = self.config
        test_config['tables'] = "[\
            {\
                \"table_name\":\"test1\",\
                \"search_prefix\":\"\",\
                \"search_pattern\":\"txt\",\
                \"key_properties\":[],\
                \"delimiter\":\",\",\
                \"date_overrides\":\"date\"\
            },\
            {\
                \"table_name\":\"test1\",\
                \"search_prefix\":\"\",\
                \"search_pattern\":\"csv\",\
                \"key_properties\":[],\
                \"delimiter\":\",\",\
                \"date_overrides\":\"date\"\
            }\
            ]"

        calls = [
            call('Syncing table "%s".', 'test1'),
            call(
                'Getting files modified since %s.',
                datetime.datetime(2020, 1, 1, 0, 0, tzinfo=datetime.timezone.utc)
                ),
            call("Multiple table configurations found for '%s', skipping stream", 'test1')
            ]

        sync_stream(test_config, state={'start_date': '2020-01-01'}, stream = Stream("test1"))

        self.assertEqual(mocked_logger.call_count,3)
        mocked_logger.assert_has_calls(calls, any_order= True)

    @mock.patch("tap_sftp.client.SFTPConnection.sftp")
    @mock.patch("tap_sftp.client.SFTPConnection.get_files")
    @mock.patch("tap_sftp.sync.LOGGER")
    def test_sync_stream(self, mocked_logger, mocked_get_files, mocked_connect):
        '''
            Test case to verify that records are fetched from files and
            total number of records fetched is returned.
        '''
        mocked_get_files.return_value = [
            {'filepath': '/test.txt', 'last_modified':datetime.datetime.now()}
            ]
        records = sync_stream(self.config, state={'start_date': '2020-01-01'},
                                stream = Stream("data"))

        self.assertEqual(type(records), int)

    @mock.patch("singer_encodings.csv.get_row_iterators")
    @mock.patch("tap_sftp.client.SFTPConnection.sftp")
    @mock.patch("tap_sftp.sync.LOGGER")
    def test_sync_file(self, mocked_logger, mocked_connection, mocked_iterator):
        '''
            Test case to verify that a file is synced by fetching records from it
            and total number of records fetched is returned.
        '''

        file = {'filepath': '/test.txt', 'last_modified':datetime.datetime.now()}
        table_spec = {
            "table_name":"data",
            "search_prefix":"",
            "search_pattern":"txt",
            "key_properties":[],
            "delimiter":",",
            "date_overrides":"date"
            }

        # Providing a test record to verify records_synced.
        mocked_iterator.return_value = [[{"test" : "TEST"}]]

        records_synced = sync_file(mocked_connection,file, Stream("data"), table_spec)

        self.assertEqual(type(records_synced), int)
