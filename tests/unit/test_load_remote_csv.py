"""
Unit Tests for Load Remote CSV Module
=====================================
"""

import os
import tempfile
import unittest
from unittest.mock import patch, MagicMock

import pytest

# Add src to path for imports
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from scripts.ingest.load_remote_csv import (
    load_remote_csv,
    validate_csv,
    DataIngestionError
)


class TestLoadRemoteCSV(unittest.TestCase):
    """Test cases for load_remote_csv function."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.test_url = "https://example.com/data.csv"
        self.output_path = os.path.join(self.temp_dir, "test_output.csv")
    
    def tearDown(self):
        """Clean up test fixtures."""
        if os.path.exists(self.temp_dir):
            for f in os.listdir(self.temp_dir):
                os.remove(os.path.join(self.temp_dir, f))
            os.rmdir(self.temp_dir)
    
    @patch('scripts.ingest.load_remote_csv.requests.get')
    def test_load_remote_csv_success(self, mock_get):
        """Test successful CSV download."""
        # Setup mock
        mock_response = MagicMock()
        mock_response.content = b"id,name,value\n1,test,100\n2,test2,200"
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response
        
        # Execute
        result = load_remote_csv(
            url=self.test_url,
            output_path=self.output_path,
            max_retries=1
        )
        
        # Verify
        self.assertTrue(os.path.exists(result))
        self.assertEqual(os.path.getsize(result), 43)
    
    @patch('scripts.ingest.load_remote_csv.requests.get')
    def test_load_remote_csv_timeout(self, mock_get):
        """Test handling of timeout errors."""
        import requests
        mock_get.side_effect = requests.exceptions.Timeout()
        
        with self.assertRaises(DataIngestionError) as context:
            load_remote_csv(
                url=self.test_url,
                output_path=self.output_path,
                max_retries=1
            )
        
        self.assertIn("Timeout", str(context.exception))
    
    @patch('scripts.ingest.load_remote_csv.requests.get')
    def test_load_remote_csv_http_error(self, mock_get):
        """Test handling of HTTP errors."""
        import requests
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("404 Not Found")
        mock_get.return_value = mock_response
        
        with self.assertRaises(DataIngestionError) as context:
            load_remote_csv(
                url=self.test_url,
                output_path=self.output_path,
                max_retries=1
            )
        
        self.assertIn("404", str(context.exception))


class TestValidateCSV(unittest.TestCase):
    """Test cases for validate_csv function."""
    
    def test_validate_csv_exists(self):
        """Test validation of existing file."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            f.write("id,name\n1,test")
            temp_path = f.name
        
        try:
            self.assertTrue(validate_csv(temp_path))
        finally:
            os.remove(temp_path)
    
    def test_validate_csv_not_exists(self):
        """Test validation of non-existing file."""
        self.assertFalse(validate_csv("/nonexistent/file.csv"))
    
    def test_validate_csv_empty(self):
        """Test validation of empty file."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as f:
            temp_path = f.name
        
        try:
            self.assertFalse(validate_csv(temp_path))
        finally:
            os.remove(temp_path)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
