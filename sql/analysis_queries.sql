-- analysis_queries.sql
-- --------------------
-- Analytical SQL queries for the MovieLens dataset.
-- These queries are written for BigQuery SQL syntax.
-- Run these after loading data into BigQuery.
--
-- Questions answered:
--   1. What are the top 10 highest rated movies?
--   2. How do ratings vary by genre?
--   3. Which users are the most active raters?
--   4. How do ratings trend over time?
--   5. What is the rating distribution?
--   6. Which genres are most popular by decade?
--   7. Weekend vs weekday rating behaviour


-- ── QUERY 1: Top 10 highest rated movies ─────────────────────────────────────
-- Only include movies with at least 50 ratings
-- A movie with 1 five-star rating would rank first without this filter
-- This is called a "minimum support" filter — common in recommendation systems
SELECT
    m.title,
    m.release_year,
    COUNT(r.rating_id)        AS total_ratings,
    ROUND(AVG(r.rating), 2)   AS avg_rating,
    MIN(r.rating)             AS min_rating,
    MAX(r.rating)             AS max_rating
FROM FACT_RATINGS r
JOIN DIM_MOVIES   m ON r.movie_id = m.movie_id
GROUP BY m.title, m.release_year
HAVING COUNT(r.rating_id) >= 50
ORDER BY avg_rating DESC
LIMIT 10;


-- ── QUERY 2: Average rating by genre ─────────────────────────────────────────
-- Joins fact table through DIM_MOVIES to DIM_GENRES
-- Shows which genres tend to receive higher ratings
SELECT
    g.genre_name,
    COUNT(r.rating_id)        AS total_ratings,
    ROUND(AVG(r.rating), 2)   AS avg_rating,
    COUNT(DISTINCT r.movie_id) AS unique_movies
FROM FACT_RATINGS r
JOIN DIM_GENRES   g ON r.movie_id = g.movie_id
GROUP BY g.genre_name
ORDER BY avg_rating DESC;


-- ── QUERY 3: Most active users ────────────────────────────────────────────────
-- Identifies power users — useful for recommendation systems
-- Heavy raters often have well-defined taste profiles
SELECT
    u.user_id,
    u.age,
    u.gender,
    u.occupation,
    COUNT(r.rating_id)       AS total_ratings,
    ROUND(AVG(r.rating), 2)  AS avg_rating_given
FROM FACT_RATINGS r
JOIN DIM_USERS    u ON r.user_id = u.user_id
GROUP BY u.user_id, u.age, u.gender, u.occupation
ORDER BY total_ratings DESC
LIMIT 20;


-- ── QUERY 4: Ratings trend by month ──────────────────────────────────────────
-- Shows how rating volume and average score changed over time
-- Useful for identifying seasonal patterns
SELECT
    d.year,
    d.month,
    d.month_name,
    COUNT(r.rating_id)       AS total_ratings,
    ROUND(AVG(r.rating), 2)  AS avg_rating
FROM FACT_RATINGS r
JOIN DIM_DATE     d ON r.date_id = d.date_id
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;


-- ── QUERY 5: Rating distribution ─────────────────────────────────────────────
-- Shows the frequency of each rating value (1 through 5)
-- A healthy dataset should have ratings spread across all values
SELECT
    rating,
    COUNT(*)                              AS count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
    -- OVER () is a window function — calculates total across all rows
    -- This lets us get the percentage without a subquery
FROM FACT_RATINGS
GROUP BY rating
ORDER BY rating;


-- ── QUERY 6: Most popular genres by decade ───────────────────────────────────
-- Shows how genre popularity shifted across decades
-- Requires release_year from DIM_MOVIES
SELECT
    (m.release_year / 10) * 10   AS decade,
    -- Integer division rounds down to the decade start
    -- e.g 1995 / 10 = 199, * 10 = 1990
    g.genre_name,
    COUNT(r.rating_id)            AS total_ratings,
    ROUND(AVG(r.rating), 2)       AS avg_rating
FROM FACT_RATINGS  r
JOIN DIM_MOVIES    m ON r.movie_id = m.movie_id
JOIN DIM_GENRES    g ON r.movie_id = g.movie_id
WHERE m.release_year > 0          -- exclude movies with unknown release year
GROUP BY decade, g.genre_name
ORDER BY decade, total_ratings DESC;


-- ── QUERY 7: Weekend vs weekday ratings ──────────────────────────────────────
-- Tests whether people rate movies differently on weekends
-- A DE might build this to feed a business dashboard
SELECT
    d.is_weekend,
    CASE WHEN d.is_weekend THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    COUNT(r.rating_id)       AS total_ratings,
    ROUND(AVG(r.rating), 2)  AS avg_rating
FROM FACT_RATINGS r
JOIN DIM_DATE     d ON r.date_id = d.date_id
GROUP BY d.is_weekend
ORDER BY d.is_weekend;