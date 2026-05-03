"""
load.py
-------
Responsible for loading transformed DataFrames into local Parquet files.

This is the L in ELT (Extract, Load, Transform).

Why Parquet?
- Parquet is a columnar storage format widely used in DE
- Much faster to query than CSV because it stores data by column
- Compresses data efficiently — often 5-10x smaller than CSV
- Supports schema enforcement natively
- Used by BigQuery, Spark, and most modern data platforms
- When you're ready to connect BigQuery this module
  will be updated to push directly to BigQuery tables

Why local first?
- Lets you verify the pipeline works before spending cloud credits
- Parquet files can be uploaded to GCP Cloud Storage then loaded
  into BigQuery with one command when ready
- Good practice to test locally before deploying to cloud

Parquet write modes:
- "overwrite" → replace existing data completely (used here)
- "append"    → add new rows to existing data
- "ignore"    → skip if data already exists
- "error"     → fail if data already exists (default)
"""

from pyspark.sql import DataFrame
from utils.logger import get_logger
import os

logger = get_logger(__name__)


def write_parquet(
    df: DataFrame,
    output_dir: str,
    table_name: str,
    mode: str = "overwrite",
) -> None:
    """
    Writes a DataFrame to Parquet format.

    Parquet files are written as a directory containing multiple
    part files — this is normal Spark behavior. Spark writes data
    in parallel across partitions, each partition becomes a part file.

    Args:
        df         (DataFrame): Transformed DataFrame to write
        output_dir (str):       Base output directory path
        table_name (str):       Name for the output folder
                                e.g "dim_users" creates output/dim_users/
        mode       (str):       Write mode — overwrite, append, ignore, error

    Returns:
        None

    Example output structure:
        data/output/dim_users/
            _SUCCESS
            part-00000-xxx.snappy.parquet
            part-00001-xxx.snappy.parquet
    """
    output_path = os.path.join(output_dir, table_name)
    logger.info(f"Writing {table_name} to Parquet: {output_path}")

    (
        df.write
        .mode(mode)
        # snappy compression — fast and widely supported
        # alternatives: gzip (smaller but slower), lz4 (fastest)
        .option("compression", "snappy")
        .parquet(output_path)
    )

    logger.info(f"Successfully wrote {table_name} to {output_path}")


def load_all(
    transformed_data: dict,
    output_dir: str = "data/output",
) -> None:
    """
    Loads all transformed DataFrames to Parquet files.

    This is the main entry point called by main.py.
    Creates one Parquet directory per table in our star schema.

    Args:
        transformed_data (dict): Output from transform.transform_all()
        output_dir       (str):  Base directory for output files

    Returns:
        None

    Output structure:
        data/output/
            dim_users/
            dim_movies/
            dim_genres/
            dim_date/
            fact_ratings/
    """
    logger.info("Starting load phase — writing all tables to Parquet")

    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Write each table
    # The key in transformed_data matches the table name
    for table_name, df in transformed_data.items():
        write_parquet(
            df=df,
            output_dir=output_dir,
            table_name=table_name,
        )

    logger.info(f"Load complete — all tables written to {output_dir}/")
    logger.info("Tables written:")
    for table_name in transformed_data.keys():
       logger.info(f"  -> {output_dir}/{table_name}/")