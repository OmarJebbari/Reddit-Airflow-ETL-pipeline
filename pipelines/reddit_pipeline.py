import logging
from etls.reddit_etl import extract_reddit_data_scraping, transform_reddit_data, load_data_to_csv
from utils.constants import OUTPUT_PATH

def reddit_pipeline(file_name: str, subreddit: str, time_filter='day', limit=100):
    logging.info(f"Starting Reddit Pipeline for subreddit: {subreddit}")
    
    # Extraction (Scraping)
    raw_data = extract_reddit_data_scraping(subreddit, time_filter, limit)
    
    # Transformation
    df = transform_reddit_data(raw_data)
    
    # Loading to CSV
    output_path = f"{OUTPUT_PATH}/{file_name}.csv"
    # Using airflow container specific path or local if outside. We can parameterize it, 
    # but based on common pipelines we write to a data folder.
    load_data_to_csv(df, output_path)
    
    logging.info(f"Reddit Pipeline completed. Saved to {output_path}")
    return output_path
