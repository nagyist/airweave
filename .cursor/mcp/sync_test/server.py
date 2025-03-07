"""MCP server for testing Airweave source integrations.

This server provides two simple endpoints for testing source integrations with Airweave:
1. Checking if a source connection is established
2. Running a sync for a specific source connection
"""

import logging
import httpx
from typing import Dict, Any
from mcp.server.fastmcp import FastMCP

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("sync-test-mcp-server")

# Constants
DEFAULT_BACKEND_URL = "http://localhost:8001"  # Backend service URL for local development

# Create MCP server
mcp = FastMCP(
    name="Sync Test MCP Server",
    instructions="This server provides tools for testing Airweave source integrations.",
    port=8002,
)


@mcp.tool(
    "Check Connection",
    "Check if a source connection is established.",
)
async def check_connection(short_name: str) -> Dict[str, Any]:
    """Check if a source connection is established.

    Args:
        short_name: The short name of the source to check

    Returns:
        A dictionary containing the connection status and message
    """
    logger.info(f"Checking connection for source: {short_name}")

    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            response = await client.get(
                f"{DEFAULT_BACKEND_URL}/cursor-dev/connections/status/{short_name}"
            )
            response.raise_for_status()
            return {
                "status": "success",
                "connection_found": True,
                "message": f"Connection found for source {short_name}!",
            }
        except httpx.HTTPStatusError as e:
            # Capture the full error response
            error_detail = {}
            try:
                error_detail = e.response.json()
            except Exception:
                # If response is not JSON, use the text content
                error_detail = {"text": e.response.text}

            return {
                "status": "error",
                "connection_found": False,
                "status_code": e.response.status_code,
                "error_detail": error_detail,
                "message": f"No connection detected for source {short_name}",
            }
        except Exception as e:
            return {
                "status": "error",
                "connection_found": False,
                "error": str(e),
                "message": f"No connection detected for source {short_name}",
            }


@mcp.tool(
    "Run Sync",
    "Run a sync for a source integration. This will first check if a connection exists, then run the sync.",
)
async def run_sync(short_name: str) -> Dict[str, Any]:
    """Run a sync for a source integration.

    Args:
        short_name: The short name of the source to sync

    Returns:
        A dictionary containing the sync status and results
    """
    logger.info(f"Running sync for source: {short_name}")

    # First check if connection exists
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            # Check connection first
            connection_response = await client.get(
                f"{DEFAULT_BACKEND_URL}/cursor-dev/connections/status/{short_name}"
            )
            connection_response.raise_for_status()
        except httpx.HTTPStatusError as e:
            # Capture the full error response
            error_detail = {}
            try:
                error_detail = e.response.json()
            except Exception:
                # If response is not JSON, use the text content
                error_detail = {"text": e.response.text}

            logger.error(f"Error checking connection for source {short_name}: {error_detail}")
            return {
                "status": "error",
                "status_code": e.response.status_code,
                "error_detail": error_detail,
                "message": f"No connection detected for source {short_name}. Please ensure a connection is established before running a sync.",
            }
        except Exception as e:
            logger.error(f"Error checking connection for source {short_name}: {e}")
            return {
                "status": "error",
                "error": str(e),
                "message": f"No connection detected for source {short_name}. Please ensure a connection is established before running a sync.",
            }

    # If connection exists, run the sync
    async with httpx.AsyncClient(timeout=60.0) as client:
        try:
            # Call the test-sync endpoint
            sync_response = await client.post(
                f"{DEFAULT_BACKEND_URL}/cursor-dev/test-sync/{short_name}"
            )
            sync_response.raise_for_status()
            result = {"status": "success", "data": sync_response.json()}
            logger.info(f"Sync result for {short_name}: success")
            return result
        except httpx.HTTPStatusError as e:
            # Capture the full error response
            error_detail = {}
            try:
                error_detail = e.response.json()
            except Exception:
                # If response is not JSON, use the text content
                error_detail = {"text": e.response.text}

            error_result = {
                "status": "error",
                "status_code": e.response.status_code,
                "error_detail": error_detail,
            }
            logger.error(f"Sync error for {short_name}: {e.response.status_code} - {error_detail}")
            return error_result
        except Exception as e:
            error_result = {"status": "error", "error": str(e)}
            logger.error(f"Sync error for {short_name}: {e}")
            return error_result


if __name__ == "__main__":
    logger.info("Starting MCP server...")
    mcp.run(transport="sse")
