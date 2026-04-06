import requests
import pandas as pd
import logging
import sys
import os
import s3fs

def extract_reddit_data_scraping(subreddit, time_filter='day', limit=100, max_posts=100):

    url = "https://www.reddit.com/r/{subreddit}/top.json"
    
    # Reddit-approved format: <platform>:<app ID>:<version> (by /u/<username>)
    headers = {'User-Agent': 'python:airflow_reddit_etl:v1.0 (by /u/your_reddit_username)'}
    
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

        try:
            response = requests.get(url.format(subreddit=subreddit), headers=headers, params=params)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to fetch data from Reddit: {e}")
            raise

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

    # 1. Convert timestamp to readable date
    df['created_utc'] = pd.to_datetime(df['created_utc'], unit='s')

    # 2. Ensure types are correct (Very important for AWS Glue/Athena)
    df['over_18'] = df['over_18'].astype(bool)
    df['author'] = df['author'].astype(str)
    df['title'] = df['title'].astype(str)
    
    # 3. Handle the 'edited' column
    # Reddit sometimes returns a timestamp for edited, or 'False'. 
    # We convert it to a boolean to keep the schema simple.
    df['edited'] = df['edited'].apply(lambda x: x if isinstance(x, bool) else True)

    # 4. Fill missing text with empty strings
    df['text'] = df['text'].fillna('')
    
    logging.info(f"Transformation complete. Final DataFrame shape: {df.shape}")

    return df

def load_data_to_csv(data: pd.DataFrame, file_path: str):
    logging.info(f"Loading data to CSV at path: {file_path}...")
    
    # NEW CODE: Check if the folder exists, and create it if it doesn't!
    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
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