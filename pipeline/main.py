"""
main.py
-------
Entry point for the MovieLens PySpark pipeline.

This script orchestrates the full ELT pipeline:
    Extract  → Read raw MovieLens files into PySpark DataFrames
    Transform → Clean and model data into star schema tables
    Load     → Write final tables to Parquet files

How to run:
    cd movielens-pyspark-pipeline
    python pipeline/main.py

Architecture:
    main.py calls extract_all() → transform_all() → load_all()
    Each stage returns a dictionary of DataFrames
    This makes it easy to test each stage independently

Future enhancements:
    - Add BigQuery loader in load.py
    - Add Airflow DAG to schedule this pipeline
    - Add data quality checks between stages
    - Add command line arguments for data_dir and output_dir
"""

import os
import sys
import time

# Add pipeline directory to path so imports work correctly
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from extract import create_spark_session, extract_all
from transform import transform_all
from load import load_all
from utils.logger import get_logger

logger = get_logger(__name__)

# ── CONFIGURATION ─────────────────────────────────────────────────────────────
# Paths are relative to the project root
# os.path.dirname gets the directory of this file (pipeline/)
# os.path.dirname again gets the project root
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR     = os.path.join(PROJECT_ROOT, "data", "raw")
OUTPUT_DIR   = os.path.join(PROJECT_ROOT, "data", "output")


def run_pipeline() -> None:
    """
    Runs the full MovieLens ELT pipeline.

    Stages:
        1. Initialize SparkSession
        2. Extract raw data from files
        3. Transform into star schema tables
        4. Load to Parquet files
        5. Stop SparkSession

    Returns:
        None
    """
    # Track total pipeline runtime
    pipeline_start = time.time()

    logger.info("=" * 60)
    logger.info("MovieLens PySpark Pipeline Starting")
    logger.info(f"Data directory : {DATA_DIR}")
    logger.info(f"Output directory: {OUTPUT_DIR}")
    logger.info("=" * 60)

    # ── STEP 1: Initialize Spark ───────────────────────────────
    spark = create_spark_session("MovieLens-Pipeline")

    try:
        # ── STEP 2: Extract ───────────────────────────────────
        logger.info("STAGE 1/3 — Extract")
        extract_start = time.time()
        raw_data = extract_all(spark, DATA_DIR)
        logger.info(f"Extract complete in {time.time() - extract_start:.2f}s")

        # ── STEP 3: Transform ─────────────────────────────────
        logger.info("STAGE 2/3 — Transform")
        transform_start = time.time()
        transformed_data = transform_all(raw_data)
        logger.info(f"Transform complete in {time.time() - transform_start:.2f}s")

        # ── STEP 4: Load ──────────────────────────────────────
        logger.info("STAGE 3/3 — Load")
        load_start = time.time()
        load_all(transformed_data, OUTPUT_DIR)
        logger.info(f"Load complete in {time.time() - load_start:.2f}s")

        # ── PIPELINE COMPLETE ─────────────────────────────────
        total_time = time.time() - pipeline_start
        logger.info("=" * 60)
        logger.info(f"Pipeline complete in {total_time:.2f}s")
        logger.info("Output tables:")
        for table in transformed_data.keys():
            logger.info(f"  -> {OUTPUT_DIR}/{table}/")
        logger.info("=" * 60)

    except Exception as e:
        # Catch any unexpected error, log it clearly, then re-raise
        # This ensures the error appears in logs before the program exits
        logger.error(f"Pipeline failed: {str(e)}")
        raise

    finally:
        # Always stop the Spark session whether pipeline succeeded or failed
        # Not stopping Spark can cause resource leaks
        logger.info("Stopping SparkSession")
        spark.stop()


if __name__ == "__main__":
    # This block only runs when main.py is executed directly
    # It won't run if main.py is imported by another module
    # e.g the Airflow DAG will import run_pipeline() without
    # triggering this block
    run_pipeline()