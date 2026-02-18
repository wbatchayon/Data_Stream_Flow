"""
Unit Tests for Publish to Kafka Module
=====================================
"""

import json
import os
import tempfile
import unittest
from unittest.mock import patch, MagicMock

import pytest

# Add src to path for imports
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from scripts.stream.publish_to_kafka import (
    publish_to_kafka,
    KafkaPublishError
)


class TestPublishToKafka(unittest.TestCase):
    """Test cases for publish_to_kafka function."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        
        # Create test JSON file
        self.test_data = [
            {"id": 1, "name": "test1", "value": 100},
            {"id": 2, "name": "test2", "value": 200}
        ]
        
        self.input_path = os.path.join(self.temp_dir, "test_data.json")
        with open(self.input_path, 'w') as f:
            json.dump(self.test_data, f)
    
    def tearDown(self):
        """Clean up test fixtures."""
        if os.path.exists(self.temp_dir):
            for f in os.listdir(self.temp_dir):
                os.remove(os.path.join(self.temp_dir, f))
            os.rmdir(self.temp_dir)
    
    @patch('scripts.stream.publish_to_kafka.KafkaProducer')
    def test_publish_to_kafka_success(self, mock_producer_class):
        """Test successful Kafka publishing."""
        # Setup mock
        mock_producer = MagicMock()
        mock_producer_class.return_value = mock_producer
        
        # Mock the send future
        mock_future = MagicMock()
        mock_metadata = MagicMock()
        mock_metadata.topic = 'data_topic'
        mock_metadata.partition = 0
        mock_metadata.offset = 0
        mock_future.get.return_value = mock_metadata
        mock_producer.send.return_value = mock_future
        
        # Execute
        result = publish_to_kafka(
            input_path=self.input_path,
            bootstrap_servers='kafka:9092',
            topic='data_topic'
        )
        
        # Verify
        self.assertEqual(result, 2)
        self.assertTrue(mock_producer.send.called)
        self.assertTrue(mock_producer.flush.called)
        self.assertTrue(mock_producer.close.called)
    
    @patch('scripts.stream.publish_to_kafka.KafkaProducer')
    def test_publish_to_kafka_connection_error(self, mock_producer_class):
        """Test handling of Kafka connection errors."""
        from kafka.errors import KafkaError
        
        mock_producer_class.side_effect = KafkaError("Connection failed")
        
        with self.assertRaises(KafkaPublishError) as context:
            publish_to_kafka(
                input_path=self.input_path,
                bootstrap_servers='kafka:9092',
                topic='data_topic'
            )
        
        self.assertIn("Kafka error", str(context.exception))
    
    def test_publish_to_kafka_file_not_found(self):
        """Test handling of missing input file."""
        with self.assertRaises(KafkaPublishError) as context:
            publish_to_kafka(
                input_path="/nonexistent/file.json",
                bootstrap_servers='kafka:9092',
                topic='data_topic'
            )
        
        self.assertIn("not found", str(context.exception).lower())


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
