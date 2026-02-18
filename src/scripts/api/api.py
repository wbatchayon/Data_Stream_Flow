"""
API Module - Pipeline Trigger API
================================
Flask API to trigger the Airflow data pipeline.

Author: William BATCHAYON
Version: 1.0.0
Security: Uses subprocess instead of os.system for security
"""

import logging
import os
import subprocess
import sys
from typing import Dict, Optional

import requests
from flask import Flask, jsonify, request

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__)

# Configuration
AIRFLOW_HOST = os.getenv("AIRFLOW_HOST", "airflow")
AIRFLOW_PORT = os.getenv("AIRFLOW_PORT", "8080")
AIRFLOW_USERNAME = os.getenv("AIRFLOW_USERNAME", "admin")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD", "admin")
DAG_ID = "data_pipeline_dag"


class PipelineTriggerError(Exception):
    """Custom exception for pipeline trigger errors."""

    pass


def get_airflow_api_url(endpoint: str = "") -> str:
    """Get the Airflow API URL."""
    return f"http://{AIRFLOW_HOST}:{AIRFLOW_PORT}/api/v1/{endpoint}"


def check_airflow_health() -> bool:
    """Check if Airflow is healthy."""
    try:
        response = requests.get(f"http://{AIRFLOW_HOST}:{AIRFLOW_PORT}/health", timeout=10)
        return response.status_code == 200
    except Exception as e:
        logger.error(f"Airflow health check failed: {e}")
        return False


def trigger_airflow_dag(run_id: Optional[str] = None) -> Dict:
    """
    Trigger an Airflow DAG using the Airflow REST API.

    This is the secure replacement for os.system() calls.

    Args:
        run_id: Optional run ID for the DAG

    Returns:
        Dictionary with response data

    Raises:
        PipelineTriggerError: If triggering fails
    """
    try:
        # Use Airflow REST API (more secure than CLI)
        url = get_airflow_api_url(f"dags/{DAG_ID}/dagRuns")

        # Prepare payload
        payload = {"conf": {}, "note": f"Triggered via API by {AIRFLOW_USERNAME}"}

        if run_id:
            payload["run_id"] = run_id

        # Make the request
        response = requests.post(
            url, json=payload, auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD), timeout=30
        )

        if response.status_code == 200:
            result = response.json()
            logger.info(f"DAG triggered successfully: {result.get('dag_run_id')}")
            return {
                "status": "success",
                "message": "Pipeline triggered successfully",
                "dag_run_id": result.get("dag_run_id"),
                "execution_date": result.get("execution_date"),
            }
        elif response.status_code == 409:
            # DAG already running
            logger.warning("DAG is already running")
            return {
                "status": "warning",
                "message": "DAG is already running",
                "code": "DAG_ALREADY_RUNNING",
            }
        else:
            raise PipelineTriggerError(
                f"Failed to trigger DAG: {response.status_code} - {response.text}"
            )

    except requests.exceptions.RequestException as e:
        raise PipelineTriggerError(f"Request to Airflow failed: {e}") from e


@app.route("/health", methods=["GET"])
def health_check():
    """Health check endpoint."""
    airflow_healthy = check_airflow_health()

    return jsonify(
        {
            "status": "healthy" if airflow_healthy else "degraded",
            "services": {"airflow": "up" if airflow_healthy else "down"},
        }
    ), (200 if airflow_healthy else 503)


@app.route("/trigger-pipeline", methods=["POST"])
def trigger_pipeline():
    """
    Trigger the data pipeline.

    This endpoint triggers the Airflow DAG to start processing.

    Request body (optional):
        - run_id: Custom run ID for the DAG

    Returns:
        JSON response with the status of the trigger operation.
    """
    try:
        # Get run_id from request or generate one
        data = request.get_json() or {}
        run_id = data.get("run_id", f"api-run-{int(os.times().elapsed * 1000)}")

        logger.info(f"Triggering pipeline with run_id: {run_id}")

        # Trigger the DAG
        result = trigger_airflow_dag(run_id=run_id)

        return jsonify(result), 200

    except PipelineTriggerError as e:
        logger.error(f"Pipeline trigger failed: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return jsonify({"status": "error", "message": "An unexpected error occurred"}), 500


@app.route("/pipeline-status", methods=["GET"])
def pipeline_status():
    """
    Get the current pipeline status.

    Returns:
        JSON response with pipeline status information.
    """
    try:
        url = get_airflow_api_url(f"dags/{DAG_ID}/dagRuns")

        response = requests.get(
            url, auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD), params={"limit": 5}, timeout=10
        )

        if response.status_code == 200:
            data = response.json()
            return jsonify({"status": "success", "dag_runs": data.get("dag_runs", [])}), 200
        else:
            return (
                jsonify({"status": "error", "message": "Failed to get pipeline status"}),
                response.status_code,
            )

    except Exception as e:
        logger.error(f"Failed to get pipeline status: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/", methods=["GET"])
def root():
    """Root endpoint with API information."""
    return (
        jsonify(
            {
                "name": "Data Stream Flow API",
                "version": "1.0.0",
                "endpoints": {
                    "health": "/health",
                    "trigger": "/trigger-pipeline",
                    "status": "/pipeline-status",
                },
            }
        ),
        200,
    )


if __name__ == "__main__":
    logger.info("Starting Data Stream Flow API")
    logger.info(f"Airflow host: {AIRFLOW_HOST}:{AIRFLOW_PORT}")

    # Run the Flask app
    app.run(
        host="0.0.0.0",
        port=int(os.getenv("PORT", 5000)),
        debug=os.getenv("DEBUG", "false").lower() == "true",
    )
