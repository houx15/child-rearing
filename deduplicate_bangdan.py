"""
我有一个通过下列代码存储的parquet
def append_to_parquet(date, results):
    将数据追加到指定年份的 Parquet 文件中。
    :param year: 年份（如 2024）
    :param userids: 用户 ID 列表
    :param results: 结果数据列表
    output_parquet_path = f"{TEXT_DIR}/{date}.parquet"
    columns = ["keyword_id", "weibo_id", "user_id", "time_stamp", "is_retweet", "zhuan", "ping", "zhan", "weibo_content"]
    df = pd.DataFrame(list(results), columns=columns)

    # # 检查文件是否存在
    # if not os.path.exists(output_parquet_path):
    #     # 如果文件不存在，直接写入（新建文件）
    df.to_parquet(output_parquet_path, engine="fastparquet", index=False)

我现在需要：
遍历某一年01-01到12-31的parquet文件
如果parquet不存在，跳过
如果parquet为空，删除并跳过
对每个parquet进行weibo_id去重
"""

import os
import pandas as pd
from datetime import datetime, timedelta
import argparse

TEXT_DIR = "text_data"

def deduplicate_parquet(year):
    # 生成一个list，包括从2020-01-01到2020-12-31的日期
    start_date = datetime.date(year, 1, 1)
    end_date = datetime.date(year, 12, 31)
    date_range = [start_date + timedelta(days=n) for n in range((end_date - start_date).days + 1)]

    for date in date_range:
        date_str = date.strftime('%Y-%m-%d')
        parquet_path = f"{TEXT_DIR}/{date_str}.parquet"

        if not os.path.exists(parquet_path):
            continue

        df = pd.read_parquet(parquet_path)
        if df.empty:
            os.remove(parquet_path)
            continue

        df.drop_duplicates(subset='weibo_id', inplace=True)
        df.to_parquet(parquet_path, engine='fastparquet', index=False)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--year', type=int, required=True, default=2020)
    args = parser.parse_args()
    deduplicate_parquet(args.year)