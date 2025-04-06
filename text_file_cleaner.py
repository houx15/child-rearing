"""
1. remove duplicate ones
2. categorize all the text into different topics
3. generate 10,000 samples for each topic (for training)
"""

import os
import pandas as pd
from datetime import datetime, timedelta
import fire
import json

from collections import defaultdict

from utils.utils import weibo_text_cleaner

TEXT_DIR = "text_data"

def handle_retweet(text):
    return text.split("//")[0]


def deduplicate_parquet(year):
    start_date = datetime(year, 1, 1)
    end_date = datetime(year, 12, 31)
    date_range = [start_date + timedelta(days=n) for n in range((end_date - start_date).days + 1)]

    for date in date_range:
        date_str = date.strftime('%Y-%m-%d')
        parquet_path = f"{TEXT_DIR}/{date_str}.parquet"

        if not os.path.exists(parquet_path):
            continue

        df = pd.read_parquet(parquet_path)
        if df.empty:
            print(f"Warning: {parquet_path} is empty, removing...")
            os.remove(parquet_path)
            continue

        df.drop_duplicates(subset='weibo_id', inplace=True)
        df["original_weibo_content"] = df["weibo_content"].apply(handle_retweet)
        df["cleaned_weibo_content"] = df["original_weibo_content"].apply(weibo_text_cleaner)
        df.to_parquet(parquet_path, engine='fastparquet', index=False)



sample_count = 10000

def sample(topic_id, topic_date_count):
    """
    topic_date_count:
    key: date yyyy-mm-dd
    value: count of data, int

    weighted sampling based on the count of a particular day
    """
    topic_data_dir = f"topic_keyword_data/{topic_id}"
    if not os.path.exists(topic_data_dir):
        return None

    date_count = sorted(topic_date_count.items(), key=lambda x: x[1], reverse=True)
    total_count = sum([x[1] for x in date_count])
    sample_rate = sample_count / total_count

    samples = []

    for date_str, count in date_count:
        # date_str = date.strftime('%Y-%m-%d')
        if not os.path.exists(f"{topic_data_dir}/{date_str}.parquet"):
            continue
        data = pd.read_parquet(f"{topic_data_dir}/{date_str}.parquet", engine='fastparquet')
        sample_data = data.sample(frac=sample_rate, random_state=2025)
        samples.append(sample_data)
    
    topic_sample = pd.concat(samples)
    sample_dir = "topic_keyword_data_sample"
    if not os.path.exists(sample_dir):
        os.makedirs(sample_dir)
    topic_sample.to_parquet(f"{sample_dir}/{topic_id}.parquet", engine='fastparquet', index=False)


if __name__ == '__main__':
    fire.Fire({
        "clean": deduplicate_parquet,
        "sample": sample
    })


