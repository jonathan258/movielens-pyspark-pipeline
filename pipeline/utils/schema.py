"""
schema.py
---------
Defines the PySpark schemas for each raw data file.

Why define schemas explicitly?
- By default PySpark infers schemas by scanning the data
- Schema inference is slow on large datasets
- Explicit schemas are faster, safer, and self-documenting
- If the source data changes unexpectedly, schema validation
  will catch it immediately rather than silently loading bad data

PySpark Data Types used here:
- IntegerType()  → whole numbers
- FloatType()    → decimal numbers
- StringType()   → text
- LongType()     → large whole numbers (Unix timestamps)
- DateType()     → date values
"""

from pyspark.sql.types import (
    StructType,    # Defines the overall schema (like a table definition)
    StructField,   # Defines a single column
    IntegerType,   # Whole number column
    FloatType,     # Decimal number column
    StringType,    # Text column
    LongType,      # Large integer (used for Unix timestamps)
)

# ── RAW FILE SCHEMAS ──────────────────────────────────────────────────────────
# These match the exact structure of the raw MovieLens files
# StructField(name, dataType, nullable)
# nullable=True means the column can have missing values

# u.data — tab separated: user_id | movie_id | rating | timestamp
RATINGS_SCHEMA = StructType([
    StructField("user_id",        IntegerType(), nullable=False),
    StructField("movie_id",       IntegerType(), nullable=False),
    StructField("rating",         FloatType(),   nullable=False),
    StructField("unix_timestamp", LongType(),    nullable=False),
])

# u.user — pipe separated: user_id | age | gender | occupation | zip_code
USERS_SCHEMA = StructType([
    StructField("user_id",    IntegerType(), nullable=False),
    StructField("age",        IntegerType(), nullable=True),
    StructField("gender",     StringType(),  nullable=True),
    StructField("occupation", StringType(),  nullable=True),
    StructField("zip_code",   StringType(),  nullable=True),
])

# u.item — pipe separated, 24 columns
# First 5 are metadata, remaining 19 are genre flags (1 or 0)
MOVIES_SCHEMA = StructType([
    StructField("movie_id",     IntegerType(), nullable=False),
    StructField("title",        StringType(),  nullable=True),
    StructField("release_date", StringType(),  nullable=True),
    StructField("video_release",StringType(),  nullable=True),
    StructField("imdb_url",     StringType(),  nullable=True),
    # Genre flags — 1 means the movie belongs to that genre
    StructField("unknown",      IntegerType(), nullable=True),
    StructField("action",       IntegerType(), nullable=True),
    StructField("adventure",    IntegerType(), nullable=True),
    StructField("animation",    IntegerType(), nullable=True),
    StructField("childrens",    IntegerType(), nullable=True),
    StructField("comedy",       IntegerType(), nullable=True),
    StructField("crime",        IntegerType(), nullable=True),
    StructField("documentary",  IntegerType(), nullable=True),
    StructField("drama",        IntegerType(), nullable=True),
    StructField("fantasy",      IntegerType(), nullable=True),
    StructField("film_noir",    IntegerType(), nullable=True),
    StructField("horror",       IntegerType(), nullable=True),
    StructField("musical",      IntegerType(), nullable=True),
    StructField("mystery",      IntegerType(), nullable=True),
    StructField("romance",      IntegerType(), nullable=True),
    StructField("sci_fi",       IntegerType(), nullable=True),
    StructField("thriller",     IntegerType(), nullable=True),
    StructField("war",          IntegerType(), nullable=True),
    StructField("western",      IntegerType(), nullable=True),
])

# Genre names in the same order as the flags in MOVIES_SCHEMA
# Used during transformation to convert genre flags into rows
GENRE_COLUMNS = [
    "unknown", "action", "adventure", "animation", "childrens",
    "comedy", "crime", "documentary", "drama", "fantasy",
    "film_noir", "horror", "musical", "mystery", "romance",
    "sci_fi", "thriller", "war", "western"
]