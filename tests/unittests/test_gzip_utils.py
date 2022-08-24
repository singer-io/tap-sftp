import unittest
from unittest import mock
from parameterized import parameterized
from tap_sftp.gzip_utils import  get_file_name_from_gzfile , _read_exact

class FileHandler:
    '''
        Class to handle return values for mocked function calls.
    '''

    def __init__(self, file, fileobj):
        self.filename = file
        self.fileobj = fileobj

class TestClass(unittest.TestCase):
    '''
        Test class to verify working of functions implemented for gz_file.
    '''

    @mock.patch("gzip.GzipFile")
    def test_get_file_name_from_gzfile_returns_none(self, mocked_gzip_file):
        '''
            Test case to verify that no file name is returned.
        '''

        mocked_gzip_file.return_value = FileHandler("Test", mocked_gzip_file)
        mocked_gzip_file.read.return_value = b''
        returned = get_file_name_from_gzfile(fileobj = None)

        self.assertEqual(returned, None)

    @mock.patch("gzip.GzipFile")
    def test_get_file_name_from_gzfile_raises_gzipped_file_error(self, mocked_gzip_file):
        '''
            Test case to verify that OSError is raised if incorrect file is passed.
        '''

        with self.assertRaises(OSError) as e:
            get_file_name_from_gzfile(fileobj = None)

        self.assertTrue("Not a gzipped file" in str(e.exception))

    @mock.patch("gzip.GzipFile")
    def test_get_file_name_from_gzfile_raises_compression_method_error(self , mocked_gzip_file):
        '''
            Test case to verify that OSError is raised if file is compressed by unknown methods.
        '''

        mocked_gzip_file.return_value = FileHandler("Test", mocked_gzip_file)
        mocked_gzip_file.read.return_value = b'\037\213'

        with self.assertRaises(OSError) as e:
            get_file_name_from_gzfile(fileobj = None)

        self.assertEqual(str(e.exception), "Unknown compression method")

    @parameterized.expand([
        ["with_gzip_extension", "test.txt.gzip", "test.txt"],
        ["without_gzip_extension", "test.txt", "test.txt"]
    ])
    @mock.patch("gzip.GzipFile")
    @mock.patch("tap_sftp.gzip_utils._read_exact")
    def test_get_file_name_from_gzfile_no_flag(self,name,
                        test_value1, test_value2,  mocked_read_exact, mocked_gzip_file):
        '''
            Test case to verify file name when flag = 0.
        '''

        mocked_gzip_file.return_value = FileHandler("Test", mocked_gzip_file)
        mocked_gzip_file.read.return_value = b'\037\213'
        mocked_read_exact.return_value = b'\x08\x00\x17\x00\x00\x00\x00\x00'
        mocked_gzip_file.name = test_value1

        gz_file_name = get_file_name_from_gzfile(fileobj = "fileobj")

        self.assertEqual(gz_file_name, test_value2)

    @mock.patch("gzip.GzipFile")
    @mock.patch("tap_sftp.gzip_utils._read_exact")
    def test_get_file_name_from_gzfile_with_flag(self, mocked_read_exact, mocked_gzip_file):
        '''
            Test case to verify file name when flag != 0 .
        '''

        mocked_gzip_file.return_value = FileHandler("Test", mocked_gzip_file)
        mocked_gzip_file.read.side_effect = [b'\037\213',b'test_file', b'\000']
        mocked_read_exact.side_effect = [b'\x08\x0c\x17\x00\x00\x00\x00\x00', b'\x01\x00', b'\x01']

        gz_file_name = get_file_name_from_gzfile(fileobj = "fileobj")

        self.assertEqual(gz_file_name, "test_file")

    @mock.patch("gzip.GzipFile")
    def test_read_exact(self, mocked_gzip):
        '''
            Test case to verify that data of expected length(in bytes) is received.
        '''

        mocked_gzip.read.side_effect = [b'\x00',b'\x05\x03',b'\x01']
        data  = _read_exact(mocked_gzip , 2)

        self.assertEqual(data, b'\x00\x05\x03')

    @mock.patch("gzip.GzipFile")
    def test_read_exact_raises_error(self, mocked_gzip):
        '''
            Test case to verify EOFError is raised.
        '''

        mocked_gzip.read.side_effect = [b'\x05', b'',b'\x01']
        
        with self.assertRaises(EOFError) as e:
            _read_exact(mocked_gzip , 2)

        self.assertEqual(str(e.exception), 'Compressed file ended before the end-of-stream marker was reached')
