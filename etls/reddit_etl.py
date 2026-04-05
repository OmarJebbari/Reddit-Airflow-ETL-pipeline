import requests
import pandas as pd
import logging
import sys
import os 

def extract_reddit_data_scraping(subreddit, time_filter='day', limit=100):

    url = f"https://www.reddit.com/r/{subreddit}/top.json?t={time_filter}&limit={limit}"
    
    # Reddit-approved format: <platform>:<app ID>:<version> (by /u/<username>)
    headers = {'User-Agent': 'python:airflow_reddit_etl:v1.0 (by /u/your_reddit_username)'}
    
    logging.info(f"Starting extraction for subreddit: r/{subreddit} (limit: {limit}, time_filter: {time_filter})")
    
    # ... keep the rest of your try/except and parsing logic exactly the same! ...
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch data from Reddit: {e}")
        raise
    
    data = response.json()
    posts = data.get('data', {}).get('children', [])
    
    extracted_data = []
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
            'stickied': post_data.get('stickied')     
        })
        
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