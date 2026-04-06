{{ config(materialized='view') }}

{% set minio_endpoint = env_var('MINIO_ENDPOINT', 'http://minio:9000') | replace('http://', '') | replace('https://', '') %}
{% set bronze_bucket = env_var('MINIO_BRONZE_BUCKET', 'bronze') %}
{% set bronze_prefix = env_var('MINIO_BRONZE_PREFIX', 'reddit') %}

-- Configure DuckDB S3/httpfs runtime settings for MinIO access.
SET s3_endpoint='{{ minio_endpoint }}';
SET s3_access_key_id='{{ env_var("MINIO_ACCESS_KEY", "minioadmin") }}';
SET s3_secret_access_key='{{ env_var("MINIO_SECRET_KEY", "minioadmin") }}';
SET s3_use_ssl=false;
SET s3_url_style='path';

SELECT *
FROM read_parquet('s3://{{ bronze_bucket }}/{{ bronze_prefix }}/*.parquet', filename=true);
