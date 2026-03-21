"""
Module for downloading and loading raw data into the Bronze layer.

This script fetches data from the OpenF1 API (sessions, laps, pit stops, drivers)
and loads historical CSV data from the Kaggle/Ergast dataset. Everything is
saved in its raw format as Parquet files into the data/bronze/ directory.

Author: Thomas Istok
"""

import logging
import sys
import time
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import requests

# ---------------------------------------------------------------------------
# Logging configuration - output to console and file
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("pipeline.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants and paths
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent
BRONZE_DIR = BASE_DIR / "data" / "bronze"
ARCHIVE_DIR = BASE_DIR / "data_kaggle"

OPENF1_BASE_URL = "https://api.openf1.org/v1"

# Season and race to download sample data from
TARGET_YEAR = 2025
TARGET_SESSION_NAME = "Race"

# ---------------------------------------------------------------------------
# OpenF1 API - definition of endpoints to download
# ---------------------------------------------------------------------------
# First download all Race sessions for 2024,
# then for each session download laps, pit stops, and drivers.
API_ENDPOINTS: dict[str, dict[str, Any]] = {
    "sessions": {
        "url": f"{OPENF1_BASE_URL}/sessions",
        "params": {"year": TARGET_YEAR, "session_name": TARGET_SESSION_NAME},
    },
}

# Endpoints tied to a specific session_key (downloaded for the first session)
SESSION_ENDPOINTS: dict[str, dict[str, Any]] = {
    "laps": {
        "url": f"{OPENF1_BASE_URL}/laps",
        "params": {},
    },
    "pit": {
        "url": f"{OPENF1_BASE_URL}/pit",
        "params": {},
    },
    "drivers": {
        "url": f"{OPENF1_BASE_URL}/drivers",
        "params": {},
    },
}

# ---------------------------------------------------------------------------
# CSV files from Kaggle/Ergast dataset
# ---------------------------------------------------------------------------
CSV_FILES: list[str] = [
    "circuits.csv",
    "constructors.csv",
    "drivers.csv",
    "races.csv",
    "results.csv",
]


# ===========================================================================
# Functions for downloading API data
# ===========================================================================

def fetch_api_data(
    url: str,
    session: requests.Session,
    params: dict[str, Any] | None = None,
    max_retries: int = 3,
) -> list[dict]:
    """
    Downloads data from an OpenF1 API endpoint with retry logic and connection reuse.

    Performs HTTP GET request with error handling (timeout, HTTP status,
    invalid JSON). On HTTP 429 (rate limit), automatically waits and
    retries the request (exponential backoff).

    Args:
        url: Endpoint URL.
        session: requests.Session object for TCP connection reuse.
        params: Dictionary of query parameters.
        max_retries: Maximum number of retries on rate limit.

    Returns:
        List of dictionaries with API data, or empty list on error.
    """
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(
                "Downloading data from: %s | Params: %s (attempt %d/%d)",
                url, params, attempt, max_retries,
            )
            response = session.get(url, params=params, timeout=60)

            # Rate limit handling - wait and retry
            if response.status_code == 429:
                wait_time = 2 ** attempt
                logger.warning(
                    "Rate limit (429) from %s - waiting %ds before next attempt.",
                    url, wait_time,
                )
                time.sleep(wait_time)
                continue

            response.raise_for_status()

            data = response.json()
            logger.info("Successfully downloaded %d records from %s", len(data), url)
            return data

        except requests.exceptions.Timeout:
            logger.error("Timeout while downloading from %s", url)
        except requests.exceptions.HTTPError as e:
            logger.error(
                "HTTP error %s while downloading from %s", e.response.status_code, url
            )
        except requests.exceptions.ConnectionError:
            logger.error("Connection error to %s", url)
        except requests.exceptions.JSONDecodeError:
            logger.error("Invalid JSON response from %s", url)
        except requests.exceptions.RequestException as e:
            logger.error("Unexpected error downloading from %s: %s", url, e)
        break  # Do not retry for non-429 errors

    return []


def save_to_parquet(df: pd.DataFrame, name: str) -> None:
    """
    Saves a Pandas DataFrame as a Parquet file to the Bronze layer.

    Args:
        df: Pandas DataFrame to save.
        name: Name of the file (without extension).
    """
    if df.empty:
        logger.warning("DataFrame '%s' is empty - file will not be saved.", name)
        return

    output_path = BRONZE_DIR / f"{name}.parquet"
    df.to_parquet(output_path, engine="pyarrow", index=False)
    logger.info(
        "Saved %d rows to %s (%.1f KB)",
        len(df),
        output_path,
        output_path.stat().st_size / 1024,
    )


# ===========================================================================
# Ingestion pipeline
# ===========================================================================

def ingest_api_data() -> None:
    """
    Downloads data from OpenF1 API and saves it as Parquet to the Bronze layer.

    First downloads a list of sessions for a defined year, then for the first
    available Race session it downloads detailed data (laps, pit stops, drivers).
    Uses requests.Session to keep the HTTP connection alive.
    """
    logger.info("=" * 60)
    logger.info("BRONZE INGESTION - OpenF1 API (year %d)", TARGET_YEAR)
    logger.info("=" * 60)

    # Initialize HTTP session for faster API downloads (connection reuse)
    with requests.Session() as session:
        # 1) Download sessions
        endpoint = API_ENDPOINTS["sessions"]
        sessions_data = fetch_api_data(endpoint["url"], session=session, params=endpoint["params"])

        if not sessions_data:
            logger.error("Failed to download sessions - skipping API data.")
            return

        df_sessions = pd.DataFrame(sessions_data)
        save_to_parquet(df_sessions, "api_sessions")

        # 2) Use session_key from the first session for detailed data
        session_key = sessions_data[0].get("session_key")
        if not session_key:
            logger.error("Session missing 'session_key' - cannot download details.")
            return

        logger.info("Downloading detailed data for session_key=%s", session_key)

        # 3) For each endpoint, download data filtered by session_key
        for name, endpoint_cfg in SESSION_ENDPOINTS.items():
            params = {**endpoint_cfg["params"], "session_key": session_key}
            data = fetch_api_data(endpoint_cfg["url"], session=session, params=params)

            if data:
                df = pd.DataFrame(data)
                save_to_parquet(df, f"api_{name}")
            else:
                logger.warning("No data for endpoint '%s'.", name)


def ingest_csv_data() -> None:
    """
    Loads CSV files from Kaggle/Ergast archive and saves them as Parquet.

    By leveraging DuckDB, data is not fully loaded into RAM via Pandas,
    but directly streamed from source CSV to target Parquet format.
    This effectively prevents RAM exhaustion on large datasets, and DuckDB
    handles data type and encoding detection automatically.
    """
    logger.info("=" * 60)
    logger.info("BRONZE INGESTION - Kaggle CSV files (DuckDB streaming)")
    logger.info("=" * 60)

    for csv_file in CSV_FILES:
        csv_path = ARCHIVE_DIR / csv_file
        name = csv_path.stem  # Filename without extension
        output_path = BRONZE_DIR / f"csv_{name}.parquet"

        try:
            if not csv_path.exists():
                logger.error("File %s does not exist - skipping.", csv_path)
                continue

            logger.info("DuckDB converting (streaming) %s directly to Parquet...", csv_file)
            
            # Professional solution using DuckDB for memory-efficient processing
            try:
                query = f"COPY (SELECT * FROM read_csv_auto('{csv_path}')) TO '{output_path}' (FORMAT PARQUET);"
                duckdb.sql(query)
            except duckdb.Error as e:
                # Handling specific encodings for historical data (e.g. Nürburgring in circuits.csv)
                logger.warning("DuckDB encountered an encoding issue with %s. Falling back to latin-1.", csv_file)
                query_latin = f"COPY (SELECT * FROM read_csv_auto('{csv_path}', encoding='latin-1')) TO '{output_path}' (FORMAT PARQUET);"
                duckdb.sql(query_latin)
            
            logger.info(
                "Successfully saved to %s (%.1f KB)",
                output_path.name,
                output_path.stat().st_size / 1024,
            )

        except duckdb.Error as e:
            logger.error("DuckDB error after fallback while processing %s: %s", csv_file, e)
        except Exception as e:
            logger.error("Unexpected error processing %s: %s", csv_file, e)


def main() -> None:
    """
    Main entry point for the Bronze ingestion pipeline.

    Creates output directory and starts ingestion from both sources
    (OpenF1 API and Kaggle CSV).
    """
    logger.info("*" * 60)
    logger.info("STARTING BRONZE INGESTION PIPELINE")
    logger.info("*" * 60)

    # Create output directory
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    logger.info("Output directory: %s", BRONZE_DIR)

    # Run both sources
    ingest_api_data()
    ingest_csv_data()

    logger.info("*" * 60)
    logger.info("BRONZE INGESTION PIPELINE COMPLETED")
    logger.info("*" * 60)


if __name__ == "__main__":
    main()
