import requests
import pandas as pd
import logging
import sys
import os
import s3fs
import time
from sqlalchemy import create_engine


def _build_postgres_url() -> str:
    """Build Postgres URL from env vars, with a local-safe default for dev."""
    direct_url = os.environ.get("POSTGRES_URL")
    if direct_url:
        return direct_url

    host = os.environ.get("POSTGRES_HOST", "postgres")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db = os.environ.get("POSTGRES_DB", "airflow_reddit")
    user = os.environ.get("POSTGRES_USER", "postgres")
    password = os.environ.get("POSTGRES_PASSWORD", "postgres")
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"


def _fetch_reddit_page(url: str, headers: dict, params: dict, retries: int = 3, timeout: int = 20):
    """Fetch a Reddit page with simple retry/backoff for transient failures."""
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=timeout)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as exc:
            if attempt == retries:
                logging.error(f"Failed to fetch data from Reddit after {retries} attempts: {exc}")
                raise
            wait_seconds = attempt * 2
            logging.warning(
                f"Reddit request failed (attempt {attempt}/{retries}): {exc}. "
                f"Retrying in {wait_seconds}s..."
            )
            time.sleep(wait_seconds)

def extract_reddit_data_scraping(subreddit, time_filter='day', limit=100, max_posts=100):

    url = "https://www.reddit.com/r/{subreddit}/top.json"
    
    # Reddit-approved format: <platform>:<app ID>:<version> (by /u/<username>)
    user_agent = os.environ.get(
        "REDDIT_USER_AGENT",
        "python:airflow_reddit_etl:v1.0 (by /u/your_reddit_username)",
    )
    headers = {'User-Agent': user_agent}
    
    logging.info(
        f"Starting extraction for subreddit: r/{subreddit} "
        f"(limit: {limit}, max_posts: {max_posts}, time_filter: {time_filter})"
    )
    
    # ... keep the rest of your try/except and parsing logic exactly the same! ...
    
    extracted_data = []
    after = None
    page_size = min(100, max_posts)

    while len(extracted_data) < max_posts:
        params = {
            "t": time_filter,
            "limit": min(page_size, max_posts - len(extracted_data)),
        }
        if after:
            params["after"] = after

        response = _fetch_reddit_page(
            url=url.format(subreddit=subreddit),
            headers=headers,
            params=params,
            retries=int(os.environ.get("REDDIT_HTTP_RETRIES", "3")),
            timeout=int(os.environ.get("REDDIT_HTTP_TIMEOUT", "20")),
        )

        data = response.json()
        posts = data.get('data', {}).get('children', [])
        after = data.get('data', {}).get('after')

        if not posts:
            break

        for post in posts:
            post_data = post.get('data', {})
            extracted_data.append({
                'id': post_data.get('id'),
                'title': post_data.get('title'),
                'text': post_data.get('selftext'),
                'score': post_data.get('score'),
                'num_comments': post_data.get('num_comments'),
                'author': post_data.get('author'),
                'created_utc': post_data.get('created_utc'),
                'url': post_data.get('url'),
                'upvote_ratio': post_data.get('upvote_ratio'),
                'over_18': post_data.get('over_18'),
                'edited': post_data.get('edited'),
                'spoiler': post_data.get('spoiler'),
                'stickied': post_data.get('stickied'),
                'subreddit': subreddit,
            })

        if not after:
            break
        
    logging.info(f"Successfully extracted {len(extracted_data)} posts from r/{subreddit}.")
    return extracted_data

def transform_reddit_data(data):
    """
    Cleans the data to match the instructor's schema perfectly.
    """
    df = pd.DataFrame(data)

    if df.empty:
        logging.warning("No data found to transform! Returning empty DataFrame.")
        return df

    logging.info(f"Starting transformation for {len(df)} rows...")

    # Ensure expected columns exist even if Reddit payload is partial.
    expected_columns = [
        'id', 'title', 'text', 'score', 'num_comments', 'author', 'created_utc',
        'url', 'upvote_ratio', 'over_18', 'edited', 'spoiler', 'stickied', 'subreddit'
    ]
    for col in expected_columns:
        if col not in df.columns:
            df[col] = None

    # 1. Convert timestamp to readable date
    df['created_utc'] = pd.to_datetime(df['created_utc'], unit='s', errors='coerce')

    # 2. Ensure types are correct (Very important for AWS Glue/Athena)
    df['score'] = pd.to_numeric(df['score'], errors='coerce').fillna(0).astype(int)
    df['num_comments'] = pd.to_numeric(df['num_comments'], errors='coerce').fillna(0).astype(int)
    df['upvote_ratio'] = pd.to_numeric(df['upvote_ratio'], errors='coerce').fillna(0.0)
    df['over_18'] = df['over_18'].apply(lambda x: bool(x) if pd.notna(x) else False).astype(bool)
    df['author'] = df['author'].fillna('unknown').astype(str)
    df['title'] = df['title'].fillna('').astype(str)
    
    # 3. Handle the 'edited' column
    # Reddit sometimes returns a timestamp for edited, or 'False'. 
    # We convert it to a boolean to keep the schema simple.
    df['edited'] = df['edited'].apply(lambda x: x if isinstance(x, bool) else bool(x))

    df['spoiler'] = df['spoiler'].apply(lambda x: bool(x) if pd.notna(x) else False).astype(bool)
    df['stickied'] = df['stickied'].apply(lambda x: bool(x) if pd.notna(x) else False).astype(bool)

    # 4. Fill missing text with empty strings
    df['text'] = df['text'].fillna('')
    
    logging.info(f"Transformation complete. Final DataFrame shape: {df.shape}")

    return df

def load_data_to_csv(data: pd.DataFrame, file_path: str):
    logging.info(f"Loading data to CSV at path: {file_path}...")
    
    # NEW CODE: Check if the folder exists, and create it if it doesn't!
    directory = os.path.dirname(file_path)
    if directory and not os.path.exists(directory):
        os.makedirs(directory)
        logging.info(f"Created missing directory: {directory}")

    # Now save the file
    data.to_csv(file_path, index=False)
    logging.info(f"Data saved successfully. Total rows exported: {len(data)}")


def load_data_to_parquet_minio(data: pd.DataFrame, file_name: str) -> str:
    endpoint = os.environ.get("MINIO_ENDPOINT")
    access_key = os.environ.get("MINIO_ACCESS_KEY")
    secret_key = os.environ.get("MINIO_SECRET_KEY")

    if not endpoint or not access_key or not secret_key:
        raise ValueError("Missing MINIO_ENDPOINT or MINIO_ACCESS_KEY or MINIO_SECRET_KEY")

    bucket = os.environ.get("MINIO_BRONZE_BUCKET", "bronze")
    prefix = os.environ.get("MINIO_BRONZE_PREFIX", "reddit")

    fs = s3fs.S3FileSystem(
        key=access_key,
        secret=secret_key,
        client_kwargs={"endpoint_url": endpoint},
    )

    # Ensure the target bucket exists before writing parquet.
    if not fs.exists(bucket):
        fs.mkdir(bucket)
        logging.info(f"Created missing MinIO bucket: {bucket}")

    parquet_path = f"s3://{bucket}/{prefix}/{file_name}.parquet"
    data.to_parquet(parquet_path, index=False, filesystem=fs)
    logging.info(f"Data saved to MinIO parquet: {parquet_path}")
    return parquet_path


def load_latest_bronze_parquet_to_postgres(table_name: str = "reddit_gold") -> str:
    endpoint = os.environ.get("MINIO_ENDPOINT")
    access_key = os.environ.get("MINIO_ACCESS_KEY")
    secret_key = os.environ.get("MINIO_SECRET_KEY")

    if not endpoint or not access_key or not secret_key:
        raise ValueError("Missing MINIO_ENDPOINT or MINIO_ACCESS_KEY or MINIO_SECRET_KEY")

    bucket = os.environ.get("MINIO_BRONZE_BUCKET", "bronze")
    prefix = os.environ.get("MINIO_BRONZE_PREFIX", "reddit")

    fs = s3fs.S3FileSystem(
        key=access_key,
        secret=secret_key,
        client_kwargs={"endpoint_url": endpoint},
    )

    parquet_files = fs.glob(f"s3://{bucket}/{prefix}/*.parquet")
    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found in s3://{bucket}/{prefix}")

    latest_parquet = max(parquet_files, key=lambda p: fs.info(p).get("LastModified", ""))
    logging.info(f"Loading latest bronze parquet: {latest_parquet}")

    df = pd.read_parquet(latest_parquet, filesystem=fs)

    engine = create_engine(_build_postgres_url())
    try:
        df.to_sql(table_name, engine, if_exists="replace", index=False, method="multi", chunksize=5000)
    finally:
        engine.dispose()

    logging.info(f"Loaded {len(df)} rows into Postgres table '{table_name}'")
    return table_name