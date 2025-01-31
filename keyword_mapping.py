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

我现在有一个data/keywords_dict.json
key 是keyword id
value是
{
            "keyword": keyword,
            "synonyms": synonyms,
            "antonyms": antonyms
        }

我现在需要遍历某一年从2020-01-01.parquet到2020-12-31.parquet
对每个parquet文件中的weibo_content进行处理
如果keyword存在于这个weibo_content中
那么就将这个content写入一个以keyword id命名的txt文件中
"""

import os
import json
import pandas as pd
import argparse
from datetime import datetime, timedelta
from collections import defaultdict

TEXT_DIR = "text_data"
OUTPUT_DIR = "keyword_data"

def get_keywords_dict():
    """加载关键词字典并预处理"""
    with open('data/keywords_dict.json', 'r') as f:
        keywords_dict = json.load(f)
    for k, v in keywords_dict.items():
        # 将同义词和反义词合并为集合，便于快速匹配
        keywords_dict[k]['all_keywords'] = set(v['synonyms'] + v['antonyms'])
    return keywords_dict

def process_parquet(year, output_suffix=""):
    """处理指定年份的 Parquet 文件"""
    keywords_dict = get_keywords_dict()
    full_keywords = set()
    for kid, info in keywords_dict.items():
        full_keywords.update(info['all_keywords'])

    # 生成日期范围
    start_date = datetime(year, 1, 1)
    end_date = datetime(year, 12, 31)
    date_range = pd.date_range(start=start_date, end=end_date)

    keywords_count = defaultdict(int)

    # 使用 defaultdict 存储匹配结果
    keyword_texts = defaultdict(set)

    # 遍历日期范围
    for date in date_range:
        date_str = date.strftime('%Y-%m-%d')
        parquet_path = f"{TEXT_DIR}/{date_str}.parquet"

        # 如果文件不存在，跳过
        if not os.path.exists(parquet_path):
            continue

        # 读取 Parquet 文件
        df = pd.read_parquet(parquet_path)

        # 遍历关键词字典
        for kid, info in keywords_dict.items():
            keywords = info['all_keywords']  # 获取关键词集合
            for content in df['weibo_content']:
                # 使用集合匹配提高效率
                content = content.split('//')[0]  # 去除微博内容中的转发
                if any(keyword in content for keyword in keywords):
                    keyword_texts[kid].add(content)
                    for single_keyword in full_keywords:
                        if single_keyword in content:
                            keywords_count[single_keyword] += 1   

    # 将匹配结果写入文件
    for kid, texts in keyword_texts.items():
        keyword_path = f"{OUTPUT_DIR}/{output_suffix}{kid}.txt"
        with open(keyword_path, 'w') as f:
            f.write('\n'.join(texts))
    
    with open(f"{OUTPUT_DIR}/{output_suffix}keywords_count.json", 'w') as f:
        json.dump(keywords_count, f, ensure_ascii=False)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--year', type=int, required=True, default=2020)
    args = parser.parse_args()
    process_parquet(args.year, f"{args.year}-")