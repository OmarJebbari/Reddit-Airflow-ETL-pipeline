import logging
from etls.reddit_etl import (
    extract_reddit_data_scraping,
    transform_reddit_data,
    load_data_to_csv,
    load_data_to_parquet_minio,
)
from utils.constants import OUTPUT_PATH

def reddit_pipeline(file_name: str, subreddit, time_filter='day', limit=100, max_posts=100):
    subreddits = subreddit if isinstance(subreddit, list) else [subreddit]
    logging.info(f"Starting Reddit Pipeline for subreddits: {subreddits}")

    raw_data = []
    for sub in subreddits:
        raw_data.extend(extract_reddit_data_scraping(sub, time_filter, limit, max_posts))

    df = transform_reddit_data(raw_data)
    
    # Loading to CSV
    output_path = f"{OUTPUT_PATH}/{file_name}.csv"
    # Using airflow container specific path or local if outside. We can parameterize it, 
    # but based on common pipelines we write to a data folder.
    load_data_to_csv(df, output_path)

    # Loading to MinIO (Bronze parquet)
    minio_path = load_data_to_parquet_minio(df, file_name)
    
    logging.info(f"Reddit Pipeline completed. Saved to {output_path} and {minio_path}")
    return output_path
