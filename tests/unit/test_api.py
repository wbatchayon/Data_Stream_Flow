"""
Unit Tests for API Module
"""

import os

# Add src to path for imports
import sys
import unittest
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))


class TestAPI(unittest.TestCase):
    """Test cases for API functions."""

    def setUp(self):
        """Set up test fixtures."""
        # Mock environment variables
        os.environ["AIRFLOW_HOST"] = "test-airflow"
        os.environ["AIRFLOW_PORT"] = "8080"
        os.environ["AIRFLOW_USERNAME"] = "testuser"
        os.environ["AIRFLOW_PASSWORD"] = "testpass"

    def tearDown(self):
        """Clean up test fixtures."""
        pass

    @patch("scripts.api.api.requests.get")
    def test_check_airflow_health_success(self, mock_get):
        """Test successful Airflow health check."""
        from scripts.api.api import check_airflow_health

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_get.return_value = mock_response

        result = check_airflow_health()

        self.assertTrue(result)

    @patch("scripts.api.api.requests.get")
    def test_check_airflow_health_failure(self, mock_get):
        """Test failed Airflow health check."""
        from scripts.api.api import check_airflow_health

        mock_get.side_effect = Exception("Connection refused")

        result = check_airflow_health()

        self.assertFalse(result)

    @patch("scripts.api.api.requests.post")
    @patch("scripts.api.api.requests.get")
    def test_trigger_airflow_dag_success(self, mock_get, mock_post):
        """Test successful DAG trigger."""
        from scripts.api.api import trigger_airflow_dag

        # Mock health check
        mock_health = MagicMock()
        mock_health.status_code = 200
        mock_get.return_value = mock_health

        # Mock trigger response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "dag_run_id": "test-run-123",
            "execution_date": "2025-01-01T00:00:00",
        }
        mock_post.return_value = mock_response

        result = trigger_airflow_dag(run_id="test-run-123")

        self.assertEqual(result["status"], "success")
        self.assertIn("dag_run_id", result)

    @patch("scripts.api.api.requests.post")
    @patch("scripts.api.api.requests.get")
    def test_trigger_airflow_dag_already_running(self, mock_get, mock_post):
        """Test DAG already running."""
        from scripts.api.api import trigger_airflow_dag

        # Mock health check
        mock_health = MagicMock()
        mock_health.status_code = 200
        mock_get.return_value = mock_health

        # Mock 409 response (conflict - DAG already running)
        mock_response = MagicMock()
        mock_response.status_code = 409
        mock_response.text = "DAG already running"
        mock_post.return_value = mock_response

        result = trigger_airflow_dag(run_id="test-run-123")

        self.assertEqual(result["status"], "warning")
        self.assertEqual(result["code"], "DAG_ALREADY_RUNNING")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
