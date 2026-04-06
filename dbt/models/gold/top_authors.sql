{{ config(materialized='table') }}

WITH silver_data AS (
    SELECT * FROM {{ ref('silver_reddit_cleaned') }}
)

SELECT
    author,
    COUNT(*) AS post_count
FROM silver_data
WHERE author != '[deleted]'
GROUP BY author
ORDER BY post_count DESC
