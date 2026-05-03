"""
extract.py
----------
Responsible for reading raw MovieLens files into PySpark DataFrames.

This is the E in ELT (Extract, Load, Transform).

What this module does:
- Initializes a PySpark session (the entry point to all Spark operations)
- Reads each raw data file into a DataFrame using explicit schemas
- Returns raw unmodified DataFrames to the caller

Why we keep extraction separate from transformation:
- Single responsibility principle — each module does one job
- If the source data format changes, we only update this file
- Makes testing easier — we can test extraction independently
- Mirrors how real DE teams structure production pipelines

PySpark Session:
- SparkSession is the entry point to PySpark
- Think of it as the "connection" to your Spark engine
- You only ever need one per pipeline run
- .master("local[*]") means run locally using all available CPU cores
- In production this would point to a cloud Spark cluster (GCP Dataproc)
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from utils.logger import get_logger
from utils.schema import (
    RATINGS_SCHEMA,
    USERS_SCHEMA,
    MOVIES_SCHEMA,
)
import os
# Windows requires winutils.exe and hadoop.dll to write files
# This tells PySpark where to find the Hadoop native binaries
# This is a Windows-only requirement — on Linux/Mac this is not needed
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = os.environ["PATH"] + r";C:\hadoop\bin"

# Initialize logger for this module
# __name__ gives the logger the name of this file (extract)
# so log lines show: "extract | INFO | message"
logger = get_logger(__name__)


def create_spark_session(app_name: str = "MovieLens Pipeline") -> SparkSession:
    """
    Creates and returns a SparkSession.

    SparkSession is the unified entry point for all PySpark operations.
    It replaces the older SparkContext and SQLContext from earlier Spark versions.

    Args:
        app_name (str): Name shown in the Spark UI and logs.
                        Helps identify your job when running on a cluster.

    Returns:
        SparkSession: Active Spark session

    Notes:
        - .getOrCreate() returns existing session if one already exists
          This is important — you never want two Spark sessions running
        - .master("local[*]") runs Spark locally
          [*] means use all available CPU cores
          In production: .master("yarn") or .master("spark://host:port")
        - .config("spark.sql.legacy.timeParserPolicy", "LEGACY") handles
          date parsing differences between Spark versions
    """
    logger.info(f"Initializing SparkSession: {app_name}")

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .config("spark.sql.shuffle.partitions", "8")  
        # shuffle.partitions controls parallelism during joins/aggregations
        # Default is 200 which is too high for small datasets
        # 8 is appropriate for 100K rows on a local machine
        # On a cluster with 10M+ rows you'd set this much higher
        .getOrCreate()
    )

    # Set log level to WARN to reduce Spark's own verbose output
    # Spark logs a LOT — this keeps your terminal readable
    spark.sparkContext.setLogLevel("WARN")

    logger.info("SparkSession created successfully")
    return spark


def read_file(
    spark: SparkSession,
    filepath: str,
    schema: StructType,
    separator: str,
    has_header: bool = False,
) -> DataFrame:
    """
    Generic file reader that returns a PySpark DataFrame.

    This is a helper function used by all the specific readers below.
    By centralizing file reading logic here we avoid repeating the same
    options in every function — DRY principle (Don't Repeat Yourself).

    Args:
        spark       (SparkSession): Active Spark session
        filepath    (str):          Full path to the data file
        schema      (StructType):   Explicit schema from schema.py
        separator   (str):          Column delimiter e.g "\\t" or "|"
        has_header  (bool):         Whether first row is a header row

    Returns:
        DataFrame: Raw PySpark DataFrame matching the provided schema

    Raises:
        FileNotFoundError: If the file path does not exist
    """

    # Validate file exists before attempting to read
    # Better to fail fast with a clear message than let Spark
    # throw a confusing Java exception
    if not os.path.exists(filepath):
        logger.error(f"File not found: {filepath}")
        raise FileNotFoundError(f"Data file not found: {filepath}")

    logger.info(f"Reading file: {filepath}")

    df = (
        spark.read
        .format("csv")                      # treat file as CSV regardless of extension
        .option("sep", separator)           # column delimiter
        .option("header", has_header)       # first row is header or data
        .option("encoding", "ISO-8859-1")   # MovieLens uses latin-1 encoding
                                            # not UTF-8 — movie titles have
                                            # special characters like accents
        .schema(schema)                     # apply explicit schema
                                            # faster than inferSchema=True
                                            # and validates data types
        .load(filepath)
    )

    # Log row count for observability
    # In production pipelines you always want to know
    # how many rows were extracted vs how many were expected
    row_count = df.count()
    logger.info(f"Extracted {row_count:,} rows from {os.path.basename(filepath)}")

    return df


def extract_ratings(spark: SparkSession, data_dir: str) -> DataFrame:
    """
    Reads u.data — the core ratings file.

    File format: tab-separated, no header
    Columns: user_id | movie_id | rating | unix_timestamp

    This becomes FACT_RATINGS after transformation.
    It is the largest file — 100,000 rows in the 100K dataset.

    Args:
        spark    (SparkSession): Active Spark session
        data_dir (str):          Path to the raw data directory

    Returns:
        DataFrame: Raw ratings data
    """
    filepath = os.path.join(data_dir, "u.data")
    return read_file(
        spark=spark,
        filepath=filepath,
        schema=RATINGS_SCHEMA,
        separator="\t",     # tab separated
        has_header=False,
    )


def extract_users(spark: SparkSession, data_dir: str) -> DataFrame:
    """
    Reads u.user — user demographic information.

    File format: pipe-separated, no header
    Columns: user_id | age | gender | occupation | zip_code

    This becomes DIM_USERS after transformation.
    943 users in the 100K dataset.

    Args:
        spark    (SparkSession): Active Spark session
        data_dir (str):          Path to the raw data directory

    Returns:
        DataFrame: Raw user demographic data
    """
    filepath = os.path.join(data_dir, "u.user")
    return read_file(
        spark=spark,
        filepath=filepath,
        schema=USERS_SCHEMA,
        separator="|",
        has_header=False,
    )


def extract_movies(spark: SparkSession, data_dir: str) -> DataFrame:
    """
    Reads u.item — movie metadata and genre flags.

    File format: pipe-separated, no header
    Columns: movie_id | title | release_date | video_release |
             imdb_url | [19 genre flag columns]

    The 19 genre columns are binary flags (1 or 0) indicating
    whether the movie belongs to that genre. A movie can belong
    to multiple genres simultaneously.

    This becomes DIM_MOVIES and DIM_GENRES after transformation.
    1,682 movies in the 100K dataset.

    Args:
        spark    (SparkSession): Active Spark session
        data_dir (str):          Path to the raw data directory

    Returns:
        DataFrame: Raw movie metadata with genre flags
    """
    filepath = os.path.join(data_dir, "u.item")
    return read_file(
        spark=spark,
        filepath=filepath,
        schema=MOVIES_SCHEMA,
        separator="|",
        has_header=False,
    )


def extract_all(spark: SparkSession, data_dir: str) -> dict:
    """
    Runs all extractions and returns a dictionary of DataFrames.

    This is the main entry point called by main.py.
    Returning a dictionary makes it easy to pass all DataFrames
    to the transform stage without a long list of arguments.

    Args:
        spark    (SparkSession): Active Spark session
        data_dir (str):          Path to the raw data directory

    Returns:
        dict: {
            "ratings": DataFrame,
            "users":   DataFrame,
            "movies":  DataFrame,
        }
    """
    logger.info("Starting extraction of all MovieLens files")

    raw_data = {
        "ratings": extract_ratings(spark, data_dir),
        "users":   extract_users(spark, data_dir),
        "movies":  extract_movies(spark, data_dir),
    }

    logger.info("Extraction complete — all files loaded into DataFrames")
    return raw_data