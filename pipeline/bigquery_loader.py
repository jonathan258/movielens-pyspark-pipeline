"""
bigquery_loader.py
------------------
Loads transformed Parquet tables into GCP BigQuery.

This module is the cloud extension of load.py.
Instead of writing to local Parquet files, it reads
the Parquet output and pushes each table into BigQuery.

Why BigQuery?
- Fully managed cloud data warehouse
- Serverless -- no infrastructure to manage
- Scales to petabytes automatically
- SQL interface familiar to analysts
- Free tier: 10GB storage, 1TB queries per month
- Integrates natively with Looker Studio for dashboards

How this works:
- Reads each Parquet directory using pandas
- Uploads to BigQuery using the BigQuery Python client
- Uses WRITE_TRUNCATE -- replaces table data on each run
  This makes the pipeline idempotent -- safe to re-run

Idempotent means:
- Running the pipeline once gives the same result as
  running it ten times
- No duplicate data accumulates
- Safe to retry on failure

BigQuery table naming:
- project_id.dataset_id.table_id
- e.g optimum-rock-393913.movielens.fact_ratings
"""

import os
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from utils.logger import get_logger

logger = get_logger(__name__)

# ── CONFIGURATION ─────────────────────────────────────────────────────────────
PROJECT_ROOT    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CREDENTIALS_PATH = os.path.join(PROJECT_ROOT, "config", "gcp_credentials.json")
OUTPUT_DIR      = os.path.join(PROJECT_ROOT, "data", "output")

# GCP settings
GCP_PROJECT_ID  = "optimum-rock-393913"
BQ_DATASET_ID   = "movielens"

# Tables to load -- matches the output of transform.py
TABLES = [
    "dim_users",
    "dim_movies",
    "dim_genres",
    "dim_date",
    "fact_ratings",
]


def get_bigquery_client() -> bigquery.Client:
    """
    Creates and returns an authenticated BigQuery client.

    Authentication uses a service account JSON key file.
    The service account needs BigQuery Admin role to
    create tables and load data.

    In production you would use Workload Identity Federation
    instead of a key file -- but for learning a key file is fine.

    Returns:
        bigquery.Client: Authenticated BigQuery client
    """
    logger.info("Authenticating with GCP using service account credentials")

    credentials = service_account.Credentials.from_service_account_file(
        CREDENTIALS_PATH,
        # Scopes define what the service account is allowed to do
        # cloud-platform gives full GCP access within the project
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )

    client = bigquery.Client(
        project=GCP_PROJECT_ID,
        credentials=credentials,
    )

    logger.info(f"BigQuery client created for project: {GCP_PROJECT_ID}")
    return client


def read_parquet_table(table_name: str) -> pd.DataFrame:
    """
    Reads a Parquet directory into a pandas DataFrame.

    We use pandas here instead of PySpark because the
    BigQuery client library works natively with pandas.
    For very large datasets you would use the BigQuery
    Storage API with PySpark instead.

    Args:
        table_name (str): Name of the table to read
                          e.g "dim_users"

    Returns:
        pd.DataFrame: Table data as a pandas DataFrame

    Raises:
        FileNotFoundError: If the Parquet directory doesn't exist
    """
    parquet_path = os.path.join(OUTPUT_DIR, table_name)

    if not os.path.exists(parquet_path):
        logger.error(f"Parquet directory not found: {parquet_path}")
        raise FileNotFoundError(
            f"Run main.py first to generate Parquet output for {table_name}"
        )

    logger.info(f"Reading Parquet: {table_name}")

    # pandas can read an entire directory of Parquet part files
    # It automatically combines all part-xxxxx.parquet files
    df = pd.read_parquet(parquet_path)

    logger.info(f"Read {len(df):,} rows from {table_name}")
    return df


def load_table_to_bigquery(
    client: bigquery.Client,
    df: pd.DataFrame,
    table_name: str,
) -> None:
    """
    Loads a pandas DataFrame into a BigQuery table.

    Uses WRITE_TRUNCATE mode which replaces all existing data
    in the table on each load. This makes it safe to re-run.

    The full BigQuery table reference is:
        project_id.dataset_id.table_name
    e.g:
        optimum-rock-393913.movielens.fact_ratings

    Args:
        client     (bigquery.Client): Authenticated BigQuery client
        df         (pd.DataFrame):    Data to load
        table_name (str):             Target table name

    Returns:
        None
    """
    # Full table reference
    table_ref = f"{GCP_PROJECT_ID}.{BQ_DATASET_ID}.{table_name}"

    logger.info(f"Loading {len(df):,} rows to BigQuery: {table_ref}")

    # Job configuration defines how the load behaves
    job_config = bigquery.LoadJobConfig(
        # WRITE_TRUNCATE = replace existing data completely
        # WRITE_APPEND   = add rows to existing data
        # WRITE_EMPTY    = only write if table is empty
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,

        # AUTO_DETECT lets BigQuery infer schema from the DataFrame
        # In production you would define explicit schemas here
        # just like we defined explicit schemas in PySpark
        autodetect=True,
    )

    # Submit the load job
    # load_table_from_dataframe streams the pandas DataFrame to BigQuery
    load_job = client.load_table_from_dataframe(
        df,
        table_ref,
        job_config=job_config,
    )

    # Wait for the job to complete
    # BigQuery jobs are async -- .result() blocks until done
    load_job.result()

    # Verify the load by checking the table row count
    table = client.get_table(table_ref)
    logger.info(
        f"Successfully loaded {table.num_rows:,} rows to {table_ref}"
    )


def load_all_to_bigquery() -> None:
    """
    Main entry point -- loads all tables to BigQuery.

    Reads each Parquet table and loads it to BigQuery
    in the correct order:
    1. Dimension tables first
    2. Fact table last

    This order matters because in production you might
    have foreign key constraints -- dimensions must exist
    before the fact table that references them.

    Returns:
        None
    """
    logger.info("=" * 60)
    logger.info("Starting BigQuery load")
    logger.info(f"Project  : {GCP_PROJECT_ID}")
    logger.info(f"Dataset  : {BQ_DATASET_ID}")
    logger.info(f"Tables   : {', '.join(TABLES)}")
    logger.info("=" * 60)

    # Get authenticated client
    client = get_bigquery_client()

    # Load each table
    for table_name in TABLES:
        try:
            # Read from Parquet
            df = read_parquet_table(table_name)

            # Load to BigQuery
            load_table_to_bigquery(client, df, table_name)

        except Exception as e:
            logger.error(f"Failed to load {table_name}: {str(e)}")
            raise

    logger.info("=" * 60)
    logger.info("All tables loaded to BigQuery successfully")
    logger.info(f"View your data at:")
    logger.info(
        f"https://console.cloud.google.com/bigquery?project={GCP_PROJECT_ID}"
    )
    logger.info("=" * 60)


if __name__ == "__main__":
    load_all_to_bigquery()