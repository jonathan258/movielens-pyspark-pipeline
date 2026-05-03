# MovieLens PySpark ELT Pipeline

A production-grade data engineering pipeline that processes 100,000 movie ratings from the MovieLens 100K dataset using Apache PySpark, models the data into a star schema, and writes the output to Parquet files ready for cloud warehouse loading.

Built as part of a structured data engineering portfolio — designed to be scalable, well-documented, and easily replicated across different datasets.

---

## Pipeline Architecture

```
Raw MovieLens Files (u.data, u.user, u.item)
            |
        extract.py
   PySpark reads raw files
   into DataFrames using
   explicit schemas
            |
       transform.py
   Cleans and models data
   into star schema tables
            |
         load.py
   Writes tables to
   Parquet format
            |
     data/output/
  (5 Parquet directories)
```

---

## Star Schema Design

```
          DIM_USERS
          (user_id, age, gender,
           occupation, zip_code)
                |
                |
DIM_MOVIES -- FACT_RATINGS -- DIM_DATE
(movie_id,    (rating_id,    (date_id,
title,         user_id,       full_date,
release_year,  movie_id,      day, month,
release_date,  date_id,       year, quarter,
imdb_url)      rating,        day_of_week,
               unix_timestamp) is_weekend)
                |
           DIM_GENRES
           (genre_id,
            movie_id,
            genre_name)
```

This is a **star schema** — the industry standard for analytical data warehouses.
- **Fact table** stores measurable events (ratings)
- **Dimension tables** store descriptive context (who, what, when)
- Fast analytical queries — join one fact table to any dimension

---

## Project Structure

```
movielens-pyspark-pipeline/
├── .gitignore                  # Excludes data files and credentials
├── requirements.txt            # Project dependencies
├── README.md                   # This file
├── data/
│   ├── raw/                    # Raw MovieLens source files (gitignored)
│   └── output/                 # Parquet output tables (gitignored)
├── diagrams/
│   └── erd.dbml                # ERD schema (paste into dbdiagram.io)
├── sql/
│   └── analysis_queries.sql    # 7 analytical SQL queries for BigQuery
├── pipeline/
│   ├── __init__.py
│   ├── main.py                 # Pipeline entry point
│   ├── extract.py              # Stage 1: Read raw files into DataFrames
│   ├── transform.py            # Stage 2: Clean and model into star schema
│   ├── load.py                 # Stage 3: Write to Parquet
│   └── utils/
│       ├── __init__.py
│       ├── logger.py           # Centralized logging configuration
│       └── schema.py           # Explicit PySpark schemas for all files
```

---

## Dataset

**MovieLens 100K** — provided by [GroupLens Research](https://grouplens.org/datasets/movielens/100k/)

| File | Contents | Rows |
|---|---|---|
| `u.data` | User ratings | 100,000 |
| `u.user` | User demographics | 943 |
| `u.item` | Movie metadata + genre flags | 1,682 |
| `u.genre` | Genre list | 19 |

Released: April 1998. Stable benchmark dataset widely used in recommender systems research.

---

## Output Tables

| Table | Rows | Description |
|---|---|---|
| `fact_ratings` | 100,000 | One row per rating event |
| `dim_users` | 943 | User demographic attributes |
| `dim_movies` | 1,682 | Movie metadata |
| `dim_genres` | 2,893 | Normalized movie-genre pairs |
| `dim_date` | 212 | Date dimension with time attributes |

---

## Getting Started

### Prerequisites

- Python 3.9 - 3.12
- Java 8 or Java 11 (required by PySpark)
- Apache Spark (installed via pip)
- Windows users: `winutils.exe` in `C:\hadoop\bin`

### 1. Clone the repository

```bash
git clone https://github.com/your-username/movielens-pyspark-pipeline.git
cd movielens-pyspark-pipeline
##my username is 
```

### 2. Create and activate a virtual environment

```bash
python -m venv pysparkenv

# Windows
pysparkenv\Scripts\activate

# Mac/Linux
source pysparkenv/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Download the dataset

- Go to [grouplens.org/datasets/movielens/100k](https://grouplens.org/datasets/movielens/100k/)
- Download `ml-100k.zip`
- Extract and copy these files into `data/raw/`:
  - `u.data`
  - `u.user`
  - `u.item`
  - `u.genre`

### 5. Windows only — winutils setup

PySpark requires Hadoop native binaries to write files on Windows:

```powershell
mkdir C:\hadoop\bin
# Download winutils.exe and hadoop.dll from:
# https://github.com/cdarlint/winutils/tree/master/hadoop-3.3.6/bin
# Place both files in C:\hadoop\bin
```

### 6. Run the pipeline

```bash
python pipeline/main.py
```

### 7. Verify output

```bash
# Windows
dir data\output

# Mac/Linux
ls data/output
```

You should see 5 directories — one per table in the star schema.

---

## Pipeline Stages

### Stage 1 - Extract (`extract.py`)

- Initializes a local PySpark session
- Reads each raw file using explicit schemas defined in `utils/schema.py`
- Explicit schemas are faster than schema inference and validate data types
- Logs row counts for observability
- Returns a dictionary of raw DataFrames

### Stage 2 - Transform (`transform.py`)

Builds 5 tables from 3 raw files:

**DIM_USERS**
- Filters null user IDs
- Standardizes gender to uppercase
- Fills missing zip codes with UNKNOWN

**DIM_MOVIES**
- Parses release date string to proper date type
- Extracts release year as integer
- Cleans whitespace from titles

**DIM_GENRES**
- Unpivots 19 binary genre flag columns into normalized rows
- Uses PySpark `explode()` on array of structs
- Result: one row per movie-genre pair

**DIM_DATE**
- Converts Unix timestamps to dates
- Extracts day, month, year, quarter, day of week
- Adds `is_weekend` boolean flag
- Deduplicates to one row per unique date

**FACT_RATINGS**
- Joins to DIM_DATE to get `date_id` foreign key
- Validates rating values are between 1.0 and 5.0
- Adds surrogate primary key

### Stage 3 - Load (`load.py`)

- Writes each DataFrame to Parquet format with Snappy compression
- Parquet is columnar — much faster for analytical queries than CSV
- Write mode is `overwrite` — safe to re-run the pipeline multiple times
- Output organized as one directory per table

---

## Logging

The pipeline uses Python's built-in `logging` module configured in `utils/logger.py`.

Each log line shows:
```
2026-05-02 22:22:22 | extract | INFO | Extracted 100,000 rows from u.data
```

- **Timestamp** - when it happened
- **Module** - which file logged it
- **Level** - INFO, WARNING, ERROR, DEBUG
- **Message** - what happened

Log files are written to `logs/pipeline_YYYY-MM-DD.log` automatically.

---

## SQL Analysis

7 analytical queries are included in `sql/analysis_queries.sql`:

| Query | Question answered |
|---|---|
| 1 | Top 10 highest rated movies (min 50 ratings) |
| 2 | Average rating by genre |
| 3 | Most active users |
| 4 | Rating volume and average by month |
| 5 | Rating distribution (1 through 5) |
| 6 | Most popular genres by decade |
| 7 | Weekend vs weekday rating behaviour |

These are written for BigQuery SQL syntax and will be used when the pipeline is extended to load into GCP BigQuery.

---

## Tech Stack

| Tool | Purpose |
|---|---|
| Python 3.12 | Primary language |
| Apache PySpark 4.x | Distributed data processing |
| Parquet + Snappy | Columnar output format |
| Python logging | Observability and debugging |

---

## Key Concepts Demonstrated

- **Star schema design** - fact and dimension tables with foreign keys
- **Explicit schema definition** - faster and safer than schema inference
- **Data unpivoting** - converting wide genre flags to normalized rows using `explode()`
- **Date dimension** - building a reusable time table from raw timestamps
- **Surrogate keys** - generating unique IDs with `monotonically_increasing_id()`
- **Proper logging** - structured logs with levels, timestamps, and module names
- **Separation of concerns** - extract, transform, load in separate modules
- **Parquet output** - columnar format ready for BigQuery or any cloud warehouse

---

## What's Next

This is the local batch pipeline stage. Planned extensions:

- **Airflow DAG** - orchestrate the pipeline on a schedule
- **GCP BigQuery** - load Parquet tables into a cloud data warehouse
- **dbt transformations** - SQL-based transformations on top of BigQuery
- **Looker Studio dashboard** - visualize rating trends and genre popularity
- **Scale up** - run the same pipeline against MovieLens 10M and 25M datasets

---

## ERD Diagram

The full ERD is defined in `diagrams/erd.dbml` using [dbml syntax](https://dbml.dbdiagram.io/docs/).

To view it visually:
1. Go to [dbdiagram.io](https://dbdiagram.io)
2. Paste the contents of `diagrams/erd.dbml`
3. The interactive diagram renders automatically

---

## Author

Campbell - building a data engineering portfolio through real projects.

Learning path: Codecademy Data Engineer -> Hands-on projects -> Google Data Engineering Certificate

---

## Data Citation

F. Maxwell Harper and Joseph A. Konstan. 2015. The MovieLens Datasets: History and Context. ACM Transactions on Interactive Intelligent Systems (TiiS) 5, 4: 51. https://doi.org/10.1145/2827872