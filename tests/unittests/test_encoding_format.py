import unittest
import json
import sys
from unittest.mock import patch
from io import StringIO
from parameterized import parameterized
from tap_sftp import do_discover


class TestDoDiscover(unittest.TestCase):
    """Unit tests for the do_discover function."""

    def setUp(self):
        # Create an instance of StringIO to capture stdout
        self.mock_stdout = StringIO()

    def tearDown(self):
        # Reset sys.stdout to its original state
        sys.stdout = sys.__stdout__


    @patch("tap_sftp.is_valid_encoding", return_value=True)
    @patch("tap_sftp.discover_streams", return_value=["stream1", "stream2"])
    def test_do_discover_valid_encoding_utf_8(
        self, mock_discover_streams, mock_is_valid_encoding
    ):
        """Test do_discover with a valid encoding format"""
        config = {"encoding_format": "utf-8"}
        captured_output = sys.stdout

        with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
            do_discover(config)
            output = mock_stdout.getvalue().strip()
            expected_output = json.dumps(
                {"streams": ["stream1", "stream2"]}, indent=2
            )
            self.assertEqual(output, expected_output)
            mock_is_valid_encoding.assert_not_called()
        mock_discover_streams.assert_called_with(config, "utf-8")
        self.assertEqual(captured_output, sys.stdout)  # Ensure sys.stdout is restored


    @patch("tap_sftp.is_valid_encoding", return_value=True)
    @patch("tap_sftp.discover_streams", return_value=["stream1", "stream2"])
    def test_do_discover_encoding_latin_1(
        self, mock_discover_streams, mock_is_valid_encoding
    ):
        """Test do_discover with a valid encoding format"""
        config = {"encoding_format": "latin_1"}
        captured_output = sys.stdout

        with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
            do_discover(config)
            output = mock_stdout.getvalue().strip()
            expected_output = json.dumps(
                {"streams": ["stream1", "stream2"]}, indent=2
            )
            self.assertEqual(output, expected_output)

        mock_is_valid_encoding.assert_called_with("latin_1")
        self.assertEqual(captured_output, sys.stdout)  # Ensure sys.stdout is restored
    
    @patch("tap_sftp.is_valid_encoding", return_value=False)
    def test_do_discover_invalid_encoding(self, mock_is_valid_encoding):
        """Test do_discover with an invalid encoding format."""
        config = {"encoding_format": "invalid-encoding"}

        with self.assertRaises(Exception) as context:
            do_discover(config)

        self.assertIn("Unknown Encoding -", str(context.exception))

        mock_is_valid_encoding.assert_called_with("invalid-encoding")
