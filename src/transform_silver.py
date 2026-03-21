"""
Module for cleaning and transforming Bronze data into the Silver layer.

This script loads raw Parquet data from data/bronze/, performs
basic cleaning (deduplication, data type enforcement, NULL value
handling, text normalization) and saves cleaned data to data/silver/.

Author: Thomas Istok
"""

import logging
import sys
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Logging configuration
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
SILVER_DIR = BASE_DIR / "data" / "silver"


# ===========================================================================
# Helper functions
# ===========================================================================

def load_bronze(name: str) -> pd.DataFrame:
    """
    Loads a Parquet file from the Bronze layer.

    Args:
        name: Name of the file (without .parquet extension).

    Returns:
        Pandas DataFrame with loaded data.

    Raises:
        FileNotFoundError: If the file does not exist.
    """
    path = BRONZE_DIR / f"{name}.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Bronze file does not exist: {path}")

    df = pd.read_parquet(path)
    logger.info("Loaded %d rows from %s", len(df), path.name)
    return df


def save_silver(df: pd.DataFrame, name: str) -> None:
    """
    Saves a cleaned DataFrame as Parquet to the Silver layer.

    Args:
        df: Pandas DataFrame to save.
        name: Name of the file (without extension).
    """
    if df.empty:
        logger.warning("DataFrame '%s' is empty - will not be saved.", name)
        return

    output_path = SILVER_DIR / f"{name}.parquet"
    df.to_parquet(output_path, engine="pyarrow", index=False)
    logger.info(
        "Silver saved: %s (%d rows, %.1f KB)",
        output_path.name,
        len(df),
        output_path.stat().st_size / 1024,
    )


def deduplicate(df: pd.DataFrame, name: str) -> pd.DataFrame:
    """
    Removes duplicate rows and logs the number of removed rows.

    Args:
        df: Input DataFrame.
        name: Table name (for logging).

    Returns:
        DataFrame without duplicates.
    """
    before = len(df)
    df = df.drop_duplicates()
    removed = before - len(df)
    if removed > 0:
        logger.info("Removed %d duplicate rows from '%s'.", removed, name)
    return df


def strip_string_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Removes trailing whitespaces from text columns.

    Args:
        df: Input DataFrame.

    Returns:
        DataFrame with cleaned text columns.
    """
    str_cols = df.select_dtypes(include=["object", "string"]).columns
    for col in str_cols:
        df[col] = df[col].astype(str).str.strip()
        # Replace empty strings with None (for consistency)
        df[col] = df[col].replace({"": None, "nan": None, "None": None})
    return df


# ===========================================================================
# Transformation functions - each processes one Bronze table
# ===========================================================================

def transform_api_sessions() -> None:
    """Cleans race session data from OpenF1 API."""
    name = "api_sessions"
    df = load_bronze(name)
    df = deduplicate(df, name)
    df = strip_string_columns(df)

    # Data type conversion
    df["date_start"] = pd.to_datetime(df["date_start"], format="ISO8601", utc=True)
    df["date_end"] = pd.to_datetime(df["date_end"], format="ISO8601", utc=True)
    df["year"] = df["year"].astype("int32")
    df["session_key"] = df["session_key"].astype("int32")
    df["meeting_key"] = df["meeting_key"].astype("int32")
    df["circuit_key"] = df["circuit_key"].astype("int32")
    df["country_key"] = df["country_key"].astype("int32")

    save_silver(df, name)


def transform_api_laps() -> None:
    """Cleans lap time data from OpenF1 API."""
    name = "api_laps"
    df = load_bronze(name)

    # Remove segment columns (lists/arrays) - not needed in Silver
    # Mandatory before deduplication, as numpy arrays are not hashable.
    segment_cols = [c for c in df.columns if c.startswith("segments_")]
    df = df.drop(columns=segment_cols)

    df = deduplicate(df, name)

    # Data type conversion
    df["date_start"] = pd.to_datetime(df["date_start"], format="ISO8601", utc=True)
    df["session_key"] = df["session_key"].astype("int32")
    df["meeting_key"] = df["meeting_key"].astype("int32")
    df["driver_number"] = df["driver_number"].astype("int32")
    df["lap_number"] = df["lap_number"].astype("int32")

    # Handling NULL in sector speed - replacing with 0.0
    speed_cols = ["i1_speed", "i2_speed", "st_speed"]
    for col in speed_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    # Handling NULL in sector times
    duration_cols = ["duration_sector_1", "duration_sector_2", "duration_sector_3"]
    for col in duration_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    save_silver(df, name)


def transform_api_pit() -> None:
    """Cleans pit stop data from OpenF1 API."""
    name = "api_pit"
    df = load_bronze(name)
    df = deduplicate(df, name)

    # Data type conversion
    df["date"] = pd.to_datetime(df["date"], format="ISO8601", utc=True)
    df["session_key"] = df["session_key"].astype("int32")
    df["meeting_key"] = df["meeting_key"].astype("int32")
    df["driver_number"] = df["driver_number"].astype("int32")
    df["lap_number"] = df["lap_number"].astype("int32")

    # stop_duration is fully NULL - remove, pit_duration is enough
    df["pit_duration"] = pd.to_numeric(df["pit_duration"], errors="coerce")
    df["lane_duration"] = pd.to_numeric(df["lane_duration"], errors="coerce")
    df = df.drop(columns=["stop_duration"], errors="ignore")

    save_silver(df, name)


def transform_api_drivers() -> None:
    """Cleans driver data from OpenF1 API."""
    name = "api_drivers"
    df = load_bronze(name)
    df = deduplicate(df, name)
    df = strip_string_columns(df)

    # Data type conversion
    df["session_key"] = df["session_key"].astype("int32")
    df["meeting_key"] = df["meeting_key"].astype("int32")
    df["driver_number"] = df["driver_number"].astype("int32")

    # Normalize names
    df["full_name"] = df["full_name"].str.title()
    df["first_name"] = df["first_name"].str.title()
    df["last_name"] = df["last_name"].str.title()
    df["team_name"] = df["team_name"].str.strip()

    save_silver(df, name)


def transform_csv_circuits() -> None:
    """Cleans historical circuits data from Kaggle dataset."""
    name = "csv_circuits"
    df = load_bronze(name)
    df = deduplicate(df, name)
    df = strip_string_columns(df)

    # Rename columns - strip trailing space from 'Wikipedia_url '
    df.columns = [c.strip() for c in df.columns]

    # Data type conversion
    df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
    df["long"] = pd.to_numeric(df["long"], errors="coerce")

    # Normalize texts
    df["name"] = df["name"].str.strip()
    df["country"] = df["country"].str.strip()
    df["locality"] = df["locality"].str.strip()

    save_silver(df, name)


def transform_csv_constructors() -> None:
    """Cleans historical constructors (teams) data from Kaggle dataset."""
    name = "csv_constructors"
    df = load_bronze(name)
    df = deduplicate(df, name)
    df = strip_string_columns(df)

    # Handle NULL in Wikipedia_url (124 missing)
    df["Wikipedia_url"] = df["Wikipedia_url"].fillna("")

    save_silver(df, name)


def transform_csv_drivers() -> None:
    """Cleans historical drivers data from Kaggle dataset."""
    name = "csv_drivers"
    df = load_bronze(name)
    df = deduplicate(df, name)
    df = strip_string_columns(df)

    # Convert date of birth to datetime
    df["dob"] = pd.to_datetime(df["dob"], errors="coerce")

    # Normalize names
    df["givenName"] = df["givenName"].str.strip()
    df["familyName"] = df["familyName"].str.strip()

    save_silver(df, name)


def transform_csv_races() -> None:
    """Cleans historical races data from Kaggle dataset."""
    name = "csv_races"
    df = load_bronze(name)
    df = deduplicate(df, name)
    df = strip_string_columns(df)

    # Data type conversion
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["season"] = df["season"].astype("int32")
    df["round"] = df["round"].astype("int32")

    # Handle NULL in time (731 missing - older races lack time)
    df["time"] = df["time"].fillna("00:00:00")

    save_silver(df, name)


def transform_csv_results() -> None:
    """Cleans historical race results data from Kaggle dataset."""
    name = "csv_results"
    df = load_bronze(name)
    df = deduplicate(df, name)
    df = strip_string_columns(df)

    # Conversion - position can be 'R' (retired), 'D' (disqualified) etc.
    # Keep as string, but prepare numeric version
    df["position_numeric"] = pd.to_numeric(df["position"], errors="coerce")

    # Data type conversion
    df["grid"] = df["grid"].astype("int32")
    df["position_order"] = df["position_order"].astype("int32")
    df["points"] = pd.to_numeric(df["points"], errors="coerce").fillna(0.0)
    df["laps"] = df["laps"].astype("int32")

    save_silver(df, name)


# ===========================================================================
# Main Pipeline
# ===========================================================================

# Dictionary of all transformations - key = name, value = functional reference
TRANSFORMATIONS: dict[str, callable] = {
    "api_sessions": transform_api_sessions,
    "api_laps": transform_api_laps,
    "api_pit": transform_api_pit,
    "api_drivers": transform_api_drivers,
    "csv_circuits": transform_csv_circuits,
    "csv_constructors": transform_csv_constructors,
    "csv_drivers": transform_csv_drivers,
    "csv_races": transform_csv_races,
    "csv_results": transform_csv_results,
}


def main() -> None:
    """
    Main entry point for the Silver transformation pipeline.

    Iterates over all defined transformations, executes them
    and logs the results. On an error in one transformation, continues with others.
    """
    logger.info("*" * 60)
    logger.info("STARTING SILVER TRANSFORMATION PIPELINE")
    logger.info("*" * 60)

    # Create output directory
    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    logger.info("Output directory: %s", SILVER_DIR)

    success_count = 0
    error_count = 0

    for name, transform_fn in TRANSFORMATIONS.items():
        try:
            logger.info("--- Transforming: %s ---", name)
            transform_fn()
            success_count += 1
        except FileNotFoundError as e:
            logger.warning("Skipping %s: %s", name, e)
            error_count += 1
        except Exception as e:
            logger.error("Error during transformation %s: %s", name, e)
            error_count += 1

    logger.info("*" * 60)
    logger.info(
        "SILVER PIPELINE COMPLETED: %d successful, %d errors",
        success_count,
        error_count,
    )
    logger.info("*" * 60)


if __name__ == "__main__":
    main()
