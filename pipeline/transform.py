"""
transform.py
------------
Responsible for cleaning and transforming raw DataFrames into
the star schema tables defined in our ERD.

This is the T in ELT (Extract, Load, Transform).

What this module does:
- Cleans raw data (handles nulls, fixes types, standardizes formats)
- Builds dimension tables: DIM_USERS, DIM_MOVIES, DIM_DATE, DIM_GENRES
- Builds fact table: FACT_RATINGS
- Returns a dictionary of transformed DataFrames ready for loading

Star Schema recap:
- Fact table    → stores measurable events (ratings)
- Dimension tables → store descriptive context (who, what, when)
- The fact table references dimension tables via foreign keys
- This structure makes analytical queries fast and intuitive

PySpark transformations used here:
- .withColumn()         → add or replace a column
- .withColumnRenamed()  → rename a column
- .select()             → choose which columns to keep
- .filter()             → keep rows matching a condition
- .dropna()             → remove rows with null values
- .distinct()           → remove duplicate rows
- .join()               → combine two DataFrames
- functions.col()       → reference a column by name
- functions.when()      → conditional logic (like SQL CASE WHEN)
- functions.explode()   → turn an array into multiple rows
- functions.to_date()   → convert string to date type
- functions.year()      → extract year from a date
- functions.monotonically_increasing_id() → generate unique IDs
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
# functions is the PySpark equivalent of pandas operations
# It contains all built-in column transformations
# Convention is to import as F so you write F.col(), F.when() etc

from utils.logger import get_logger
from utils.schema import GENRE_COLUMNS

logger = get_logger(__name__)


def build_dim_users(users_df: DataFrame) -> DataFrame:
    """
    Transforms raw users data into DIM_USERS dimension table.

    Transformations applied:
    - Filter out rows with null user_id (can't have a user with no ID)
    - Standardize gender to uppercase (defensive — source data is clean
      but good habit to enforce consistency)
    - Fill null zip codes with 'UNKNOWN'
    - Select only the columns we need

    Args:
        users_df (DataFrame): Raw users DataFrame from extract stage

    Returns:
        DataFrame: Clean DIM_USERS ready for loading
    
    Schema:
        user_id    int     primary key
        age        int     user age
        gender     string  M or F
        occupation string  user occupation
        zip_code   string  user zip code
    """
    logger.info("Building DIM_USERS")

    dim_users = (
        users_df
        # Remove rows where user_id is null
        # user_id is our primary key — it must exist
        .filter(F.col("user_id").isNotNull())

        # Standardize gender to uppercase
        # F.upper() is equivalent to Python's .upper() string method
        .withColumn("gender", F.upper(F.col("gender")))

        # Fill null zip codes with UNKNOWN
        # .fillna() fills nulls in specific columns
        .fillna({"zip_code": "UNKNOWN", "occupation": "UNKNOWN"})

        # Select final columns in the order we want them
        .select(
            "user_id",
            "age",
            "gender",
            "occupation",
            "zip_code",
        )
    )

    logger.info(f"DIM_USERS built: {dim_users.count():,} users")
    return dim_users


def build_dim_movies(movies_df: DataFrame) -> DataFrame:
    """
    Transforms raw movies data into DIM_MOVIES dimension table.

    Transformations applied:
    - Filter nulls on movie_id
    - Extract release year from release_date string
    - Clean title by stripping whitespace
    - Select only the columns we need

    The release_date in the source is a string like "01-Jan-1995"
    We parse it to a proper date then extract the year as an integer.

    Args:
        movies_df (DataFrame): Raw movies DataFrame from extract stage

    Returns:
        DataFrame: Clean DIM_MOVIES ready for loading

    Schema:
        movie_id     int     primary key
        title        string  movie title
        release_year int     year extracted from release_date
        release_date string  original release date string
        imdb_url     string  IMDB URL
    """
    logger.info("Building DIM_MOVIES")

    dim_movies = (
        movies_df
        .filter(F.col("movie_id").isNotNull())

        # Clean title — strip leading/trailing whitespace
        .withColumn("title", F.trim(F.col("title")))

        # Parse release_date string to a proper date type
        # "dd-MMM-yyyy" matches format like "01-Jan-1995"
        .withColumn(
            "release_date_parsed",
            F.to_date(F.col("release_date"), "dd-MMM-yyyy")
        )

        # Extract year from the parsed date
        # F.year() extracts the year component as an integer
        .withColumn(
            "release_year",
            F.year(F.col("release_date_parsed"))
        )

        # Fill null release years with 0 as a sentinel value
        # This is better than dropping the row — we still want the movie
        .fillna({"release_year": 0})

        # Fill null URLs with empty string
        .fillna({"imdb_url": ""})

        .select(
            "movie_id",
            "title",
            "release_year",
            "release_date",
            "imdb_url",
        )
    )

    logger.info(f"DIM_MOVIES built: {dim_movies.count():,} movies")
    return dim_movies


def build_dim_genres(movies_df: DataFrame) -> DataFrame:
    """
    Transforms genre flag columns into a normalized DIM_GENRES table.

    This is the most complex transformation in the pipeline.

    The source data has 19 binary columns like:
        movie_id | action | comedy | drama | ...
        1        | 1      | 0      | 1     | ...

    We need to transform this into normalized rows like:
        genre_id | movie_id | genre_name
        1        | 1        | action
        2        | 1        | drama

    This process is called UNPIVOTING or MELTING the data.
    In PySpark we use F.explode() on an array to achieve this.

    Steps:
    1. For each genre column, create a (genre_name, flag) struct
    2. Combine all structs into an array
    3. Explode the array — one row per genre per movie
    4. Filter to only keep rows where flag = 1
    5. Add a surrogate primary key

    Args:
        movies_df (DataFrame): Raw movies DataFrame from extract stage

    Returns:
        DataFrame: Normalized DIM_GENRES with one row per movie-genre pair

    Schema:
        genre_id   int     surrogate primary key
        movie_id   int     foreign key to DIM_MOVIES
        genre_name string  genre name e.g action, comedy
    """
    logger.info("Building DIM_GENRES")

    # Step 1 & 2: Build an array of structs for each movie
    # For each genre column, create a struct: {genre_name, flag}
    # F.struct() creates a struct (like a mini row within a column)
    # F.array() combines all structs into a single array column
    genre_structs = F.array(*[
        F.struct(
            F.lit(genre).alias("genre_name"),   # F.lit() creates a literal value
            F.col(genre).alias("flag")          # the 0 or 1 flag value
        )
        for genre in GENRE_COLUMNS              # loop through all 19 genre names
    ])

    dim_genres = (
        movies_df
        .filter(F.col("movie_id").isNotNull())

        # Step 2: Add the array column to the DataFrame
        .withColumn("genres_array", genre_structs)

        # Step 3: Explode — turns each element of the array into its own row
        # Before explode: 1 row per movie with an array of 19 structs
        # After explode:  19 rows per movie, one per genre struct
        .withColumn("genre_struct", F.explode(F.col("genres_array")))

        # Step 4: Filter to only keep genres the movie actually belongs to
        # genre_struct.flag accesses the flag field inside the struct
        .filter(F.col("genre_struct.flag") == 1)

        # Extract genre_name from the struct
        .withColumn("genre_name", F.col("genre_struct.genre_name"))

        # Step 5: Add surrogate primary key
        # monotonically_increasing_id() generates unique IDs across partitions
        # Note: IDs are not consecutive but are guaranteed unique
        .withColumn("genre_id", F.monotonically_increasing_id())

        .select(
            "genre_id",
            "movie_id",
            "genre_name",
        )
    )

    logger.info(f"DIM_GENRES built: {dim_genres.count():,} movie-genre pairs")
    return dim_genres


def build_dim_date(ratings_df: DataFrame) -> DataFrame:
    """
    Builds DIM_DATE from timestamps in the ratings data.

    Rather than storing raw Unix timestamps in the fact table,
    we create a date dimension with useful attributes that make
    time-based analysis fast and intuitive.

    A date dimension lets analysts ask questions like:
    - "What were ratings like on weekends vs weekdays?"
    - "Which quarter had the most ratings?"
    - "How did ratings change month over month?"

    These queries would be complex with raw timestamps but trivial
    with a proper date dimension.

    Args:
        ratings_df (DataFrame): Raw ratings DataFrame from extract stage

    Returns:
        DataFrame: DIM_DATE with one row per unique date in the dataset

    Schema:
        date_id      int     surrogate key in YYYYMMDD format
        full_date    date    full date value
        day          int     day of month
        month        int     month number 1-12
        month_name   string  January, February etc
        year         int     4 digit year
        quarter      int     1, 2, 3, or 4
        day_of_week  string  Monday, Tuesday etc
        is_weekend   boolean True if Saturday or Sunday
    """
    logger.info("Building DIM_DATE")

    dim_date = (
        ratings_df
        # Convert Unix timestamp (seconds since 1970-01-01) to timestamp type
        # F.from_unixtime() converts to string first, then we cast to date
        .withColumn(
            "full_date",
            F.to_date(F.from_unixtime(F.col("unix_timestamp")))
        )

        # Keep only unique dates — we want one row per date
        .select("full_date").distinct()

        # Add date_id as YYYYMMDD integer
        # e.g 1998-03-15 becomes 19980315
        # This format is sortable and human readable
        .withColumn(
            "date_id",
            F.date_format(F.col("full_date"), "yyyyMMdd").cast("int")
        )

        # Extract individual date components
        .withColumn("day",   F.dayofmonth(F.col("full_date")))
        .withColumn("month", F.month(F.col("full_date")))
        .withColumn("year",  F.year(F.col("full_date")))

        # Month name — F.date_format with "MMMM" gives full month name
        .withColumn(
            "month_name",
            F.date_format(F.col("full_date"), "MMMM")
        )

        # Quarter — derived from month
        # months 1-3 = Q1, 4-6 = Q2, 7-9 = Q3, 10-12 = Q4
        .withColumn(
            "quarter",
            F.quarter(F.col("full_date"))
        )

        # Day of week name — "EEEE" gives full name like Monday
        .withColumn(
            "day_of_week",
            F.date_format(F.col("full_date"), "EEEE")
        )

        # Is weekend — True if Saturday or Sunday
        # F.when() is PySpark's equivalent of SQL CASE WHEN
        # dayofweek() returns 1=Sunday, 2=Monday ... 7=Saturday
        .withColumn(
            "is_weekend",
            F.when(
                F.dayofweek(F.col("full_date")).isin([1, 7]),
                True
            ).otherwise(False)
        )

        .select(
            "date_id",
            "full_date",
            "day",
            "month",
            "month_name",
            "year",
            "quarter",
            "day_of_week",
            "is_weekend",
        )

        # Sort by date for cleanliness
        .orderBy("full_date")
    )

    logger.info(f"DIM_DATE built: {dim_date.count():,} unique dates")
    return dim_date


def build_fact_ratings(
    ratings_df: DataFrame,
    dim_date: DataFrame,
) -> DataFrame:
    """
    Builds FACT_RATINGS — the central fact table of our star schema.

    The fact table stores one row per rating event.
    It references dimension tables via foreign keys:
    - user_id  → DIM_USERS
    - movie_id → DIM_MOVIES
    - date_id  → DIM_DATE

    We join to DIM_DATE here to get the date_id foreign key.
    We don't join to DIM_USERS or DIM_MOVIES because those keys
    already exist in the raw ratings data.

    Args:
        ratings_df (DataFrame): Raw ratings DataFrame
        dim_date   (DataFrame): Built DIM_DATE (to get date_id)

    Returns:
        DataFrame: FACT_RATINGS ready for loading

    Schema:
        rating_id      int    surrogate primary key
        user_id        int    FK to DIM_USERS
        movie_id       int    FK to DIM_MOVIES
        date_id        int    FK to DIM_DATE
        rating         float  rating value 1.0 to 5.0
        unix_timestamp long   original raw timestamp
    """
    logger.info("Building FACT_RATINGS")

    # First convert unix timestamp to date in ratings
    # so we can join to DIM_DATE on the date value
    ratings_with_date = ratings_df.withColumn(
        "full_date",
        F.to_date(F.from_unixtime(F.col("unix_timestamp")))
    )

    # Join ratings to DIM_DATE to get date_id
    # Left join keeps all ratings even if date somehow not in DIM_DATE
    fact_ratings = (
        ratings_with_date
        .join(
            dim_date.select("full_date", "date_id"),
            on="full_date",
            how="left"
        )

        # Filter out any ratings with null values in key columns
        .filter(F.col("user_id").isNotNull())
        .filter(F.col("movie_id").isNotNull())
        .filter(F.col("rating").isNotNull())

        # Validate rating is within expected range 1.0 to 5.0
        # Data quality check — reject invalid ratings
        .filter(F.col("rating").between(1.0, 5.0))

        # Add surrogate primary key
        .withColumn("rating_id", F.monotonically_increasing_id())

        .select(
            "rating_id",
            "user_id",
            "movie_id",
            "date_id",
            "rating",
            "unix_timestamp",
        )
    )

    logger.info(f"FACT_RATINGS built: {fact_ratings.count():,} ratings")
    return fact_ratings


def transform_all(raw_data: dict) -> dict:
    """
    Runs all transformations and returns a dictionary of clean DataFrames.

    This is the main entry point called by main.py.
    Takes the raw_data dict from extract_all() and returns
    a transformed_data dict ready for load_all().

    Args:
        raw_data (dict): Output from extract.extract_all()
                         Keys: ratings, users, movies

    Returns:
        dict: {
            "dim_users":    DataFrame,
            "dim_movies":   DataFrame,
            "dim_genres":   DataFrame,
            "dim_date":     DataFrame,
            "fact_ratings": DataFrame,
        }
    """
    logger.info("Starting all transformations")

    # Build dimension tables first
    dim_users  = build_dim_users(raw_data["users"])
    dim_movies = build_dim_movies(raw_data["movies"])
    dim_genres = build_dim_genres(raw_data["movies"])
    dim_date   = build_dim_date(raw_data["ratings"])

    # Build fact table last — it depends on dim_date
    fact_ratings = build_fact_ratings(raw_data["ratings"], dim_date)

    transformed_data = {
        "dim_users":    dim_users,
        "dim_movies":   dim_movies,
        "dim_genres":   dim_genres,
        "dim_date":     dim_date,
        "fact_ratings": fact_ratings,
    }

    logger.info("All transformations complete")
    return transformed_data