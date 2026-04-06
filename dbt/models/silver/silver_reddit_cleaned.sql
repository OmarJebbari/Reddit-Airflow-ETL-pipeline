{{ config(materialized='table') }}

WITH bronze_data AS (
    SELECT *
    FROM {{ ref('bronze_reddit_posts') }}
),

typed_data AS (
    SELECT
        id,
        COALESCE(title, '') AS title,
        COALESCE(text, '') AS text,
        TRY_CAST(score AS INT) AS score,
        TRY_CAST(num_comments AS INT) AS num_comments,
        COALESCE(author, 'unknown') AS author,
        TRY_CAST(created_utc AS TIMESTAMP) AS created_at,
        url,
        TRY_CAST(upvote_ratio AS DOUBLE) AS upvote_ratio,
        COALESCE(TRY_CAST(over_18 AS BOOLEAN), false) AS is_nsfw,
        COALESCE(TRY_CAST(edited AS BOOLEAN), false) AS is_edited,
        COALESCE(TRY_CAST(spoiler AS BOOLEAN), false) AS is_spoiler,
        COALESCE(TRY_CAST(stickied AS BOOLEAN), false) AS is_stickied,
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY TRY_CAST(created_utc AS TIMESTAMP) DESC NULLS LAST
        ) AS row_num
    FROM bronze_data
)

SELECT
    id,
    title,
    text,
    COALESCE(score, 0) AS score,
    COALESCE(num_comments, 0) AS num_comments,
    author,
    created_at,
    url,
    COALESCE(upvote_ratio, 0.0) AS upvote_ratio,
    is_nsfw,
    is_edited,
    is_spoiler,
    is_stickied,
    CURRENT_TIMESTAMP AS processed_at
FROM typed_data
WHERE id IS NOT NULL
  AND row_num = 1
  AND is_stickied = false
