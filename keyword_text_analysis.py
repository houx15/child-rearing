import pandas as pd

import os
import argparse

from datetime import datetime, timedelta

import time

import matplotlib.pyplot as plt
import seaborn as sns


def log(text, lid=None):
    output = f"logs/keyword_count_{lid}.txt" if lid is not None else "logs/log.txt"
    with open(output, "a") as f:
        f.write(f"{text}\n")

child_keywords = ["子女", "女儿", "儿子", "孙女", "孙子", "带娃", "带孩子", "养育", "养娃"]
quality_map = {"独立性": ["独立", "自主", "自理能力", "自立", "挫折教育", "娇气", "脆弱", "温室", "勇敢", "坚强", "自强", "害怕", "溺爱", "男子汉", "依赖性", "自我生存"],
"勤奋": ["努力", "刻苦", "勤劳", "坚持", "有恒心", "半途而废", "懒散", "不上进"], 
"责任感": ["责任心", "有担当", "可靠", "暖心", "逃避责任", "不负责任"], 
"对别人宽容与尊重": ["懂事", "教养", "包容", "宽容", "理解他人", "体谅"]}

TEXT_DIR = "keyword_text_data"

keyword_to_quality = {}

for quality, keywords in quality_map.items():
    for single_k in keywords:
        keyword_to_quality[single_k] = quality
keywords = list(keyword_to_quality.keys())


def load_year_line_count(year):
    """
    读取某年的文本行数
    """
    line_count_map = {}
    file_path = f"logs/line_count_{year}_2.txt"
    with open(file_path, "r") as f:
        lines = f.readlines()
        for line in lines:
            line = line.strip()
            date, count = line.split(",")
            line_count_map[date] = int(count)
    return line_count_map

def handle_retweet(text):
    return text.split("//")[0]

def single_file_preprocess(date):
    """
    params:
    date: datetime.datetime

    return:
    pd.DataFrame
    """
    if not os.path.exists("keyword_text_data_new"):
        os.makedirs("keyword_text_data_new")
    date_str = date.strftime("%Y-%m-%d")
    file_path = f"{TEXT_DIR}/{date_str}.parquet"
    new_file_path = file_path# f"keyword_text_data_new/{date_str}.parquet"
    # 如果文件不存在，返回空的dataframe
    if not os.path.exists(file_path):
        return None
    data = pd.read_parquet(file_path, engine="fastparquet")

    """
    确保 weibo_content 中包含child_keywords中至少一个关键词
    """
    data = data[data["weibo_content"].str.contains("|".join(child_keywords))]
    # 处理retweet
    data["original_weibo_content"] = data["weibo_content"].map(handle_retweet)
    data.to_parquet(new_file_path, engine="fastparquet")


def data_preprocess():
    start_date = datetime(2016, 1, 1)
    end_date = datetime(2023, 12, 31)
    date_range = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]
    for current_date in date_range:
        start_time = int(time.time())
        single_file_preprocess(current_date)
        end_time = int(time.time())
        log(f"{current_date} finished, time: {end_time - start_time}", lid="preprocess")

def single_file_analysis(date, total_count, delete_retweet = False):
    """
    params:
    date: datetime.datetime
    total_count: int

    return:
    pd.DataFrame
    """
    date_str = date.strftime("%Y-%m-%d")
    file_path = f"{TEXT_DIR}/{date_str}.parquet"
    # 如果文件不存在，返回空的dataframe
    if not os.path.exists(file_path):
        return None
    keyword_count = {keyword: 0 for keyword in keywords}
    daily_keyword_count = {}
    data = pd.read_parquet(file_path, engine="fastparquet")
    if delete_retweet:
        data = data[data["is_retweet"] == "0"]

    for keyword in keywords:
        keyword_count[keyword] = data["original_weibo_content"].str.contains(keyword).sum()
    
    # 将结果转为dataframe
    keyword_count_df = pd.DataFrame(keyword_count.items(), columns=["keyword", "count"])
    keyword_count_df["quality"] = keyword_count_df["keyword"].map(keyword_to_quality)
    keyword_count_df["date"] = date
    keyword_count_df["total_count"] = total_count

    return keyword_count_df


def year_analysis(year):
    line_count_map = load_year_line_count(year)
    start_date = datetime(year, 1, 1)
    end_date = datetime(year, 12, 31)
    date_range = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]

    year_count = []

    for current_date in date_range:
        start_time = int(time.time())

        date_str = current_date.strftime("%Y-%m-%d")
        if date_str not in line_count_map:
            continue
        total_count = line_count_map[date_str]
        result = single_file_analysis(current_date, total_count)
        if result is None:
            continue
        year_count.append(result)

        end_time = int(time.time())
        log(f"{date_str} finished, time: {end_time - start_time}", lid=year)
    
    year_count_df = pd.concat(year_count)
    year_count_df.to_parquet(f"keyword_text_data/{year}_keyword_count.parquet", engine="fastparquet")


def aggregate():
    """
    每年的统计表：keyword_text_data/{year}_keyword_count.parquet
          keyword  count   quality       date  total_count
    index                                                 
    0          独立   2603       独立性 2022-01-01     51217197
    1          自主    453       独立性 2022-01-01     51217197
    2        自理能力     17       独立性 2022-01-01     51217197
    3          自立    409       独立性 2022-01-01     51217197

    输出：（表格和图片）
    1. 按月 - 每个关键词的频率 & 百分比
    2. 按月 - 每个品质的频率 & 百分比
    3. 按年 - 每个关键词的频率 & 百分比
    4. 按年 - 每个品质的频率 & 百分比
    """
    plt.cla()
    plt.rcParams['font.sans-serif'] = ['SimHei']  # 使用黑体
    plt.rcParams['axes.unicode_minus'] = False  # 解决负号显示问题
    rc = {"font.sans-serif": "SimHei",
      "axes.unicode_minus": False}
    sns.set(rc=rc, style="whitegrid")
    
    # 合并所有年份的数据
    df_list = []
    total_count_map = {}
    for year in range(2016, 2024):
        year_file = f"{TEXT_DIR}/{year}_keyword_count.parquet"
        df = pd.read_parquet(year_file, engine="fastparquet")
        df['year'] = year
        df['month'] = pd.to_datetime(df['date']).dt.to_period("M")
        df_list.append(df)

        year_line_count = load_year_line_count(year)
        total_count_map.update(year_line_count)
    
    # 合并为一个 DataFrame
    data = pd.concat(df_list, ignore_index=True)
    
    # 创建每日总数 DataFrame
    daily_total = pd.DataFrame(list(total_count_map.items()), columns=['date', 'total_count'])
    daily_total['month'] = pd.to_datetime(daily_total['date']).dt.to_period("M")
    daily_total['year'] = pd.to_datetime(daily_total['date']).dt.year
    
    # 计算每月的 total_count（按月求和）
    monthly_total = daily_total.groupby('month')['total_count'].sum().reset_index()
    monthly_total.rename(columns={'total_count': 'monthly_total_count'}, inplace=True)
    
    # 计算每年的 total_count（按年求和）
    yearly_total = daily_total.groupby('year')['total_count'].sum().reset_index()
    yearly_total.rename(columns={'total_count': 'yearly_total_count'}, inplace=True)
    
    # 1. 按月 - 每个关键词的频率 & 百分比
    monthly_keyword = data.groupby(['month', 'keyword']).agg(
        frequency=('count', 'sum')
    ).reset_index()
    monthly_keyword = monthly_keyword.merge(monthly_total, on='month', how='left')
    monthly_keyword['proportion'] = monthly_keyword['frequency'] / monthly_keyword['monthly_total_count']
    
    # 2. 按月 - 每个品质的频率 & 百分比
    monthly_quality = data.groupby(['month', 'quality']).agg(
        frequency=('count', 'sum')
    ).reset_index()
    monthly_quality = monthly_quality.merge(monthly_total, on='month', how='left')
    monthly_quality['proportion'] = monthly_quality['frequency'] / monthly_quality['monthly_total_count']
    
    # 3. 按年 - 每个关键词的频率 & 百分比
    yearly_keyword = data.groupby(['year', 'keyword']).agg(
        frequency=('count', 'sum')
    ).reset_index()
    yearly_keyword = yearly_keyword.merge(yearly_total, on='year', how='left')
    yearly_keyword['proportion'] = yearly_keyword['frequency'] / yearly_keyword['yearly_total_count']
    
    # 4. 按年 - 每个品质的频率 & 百分比
    yearly_quality = data.groupby(['year', 'quality']).agg(
        frequency=('count', 'sum')
    ).reset_index()
    yearly_quality = yearly_quality.merge(yearly_total, on='year', how='left')
    yearly_quality['proportion'] = yearly_quality['frequency'] / yearly_quality['yearly_total_count']
    
    # 将表格保存为csv
    monthly_keyword.to_csv(f"{TEXT_DIR}/monthly_keyword_percentage.csv", index=False)
    monthly_quality.to_csv(f"{TEXT_DIR}/monthly_quality_percentage.csv", index=False)
    yearly_keyword.to_csv(f"{TEXT_DIR}/yearly_keyword_percentage.csv", index=False)
    yearly_quality.to_csv(f"{TEXT_DIR}/yearly_quality_percentage.csv", index=False)
    
    # 将 pd.Period 转换为字符串
    months_to_display = monthly_quality['month'].dt.strftime('%Y-%m').unique()  # 获取所有月份
    monthly_quality['month'] = monthly_quality['month'].astype(str)
    
    # 按月品质频率
    plt.figure(figsize=(12, 6))
    sns.lineplot(data=monthly_quality, x='month', y='proportion', hue='quality')
    plt.title("Monthly Quality Percentage")

    # 设置横轴标签
    display_indices = [i for i, label in enumerate(months_to_display) if label.endswith('-01') or label.endswith('-04') or label.endswith('-07') or label.endswith('-10')]
    display_labels = [months_to_display[i] for i in display_indices]

    # 设置xticks
    plt.xticks(ticks=display_indices, labels=display_labels, rotation=45)
    plt.legend(loc='upper right')
    plt.tight_layout()
    plt.savefig(f"{TEXT_DIR}/monthly_quality_percentage.pdf", format='pdf')

    plt.cla()

    # 按年品质百分比
    plt.figure(figsize=(12, 6))
    sns.lineplot(data=yearly_quality, x='year', y='proportion', hue='quality', linewidth=3)
    plt.title("Yearly Quality Percentage")
    plt.legend(loc='upper right')
    plt.tight_layout()
    plt.savefig(f"{TEXT_DIR}/yearly_quality_percentage.pdf", format='pdf')

    plt.cla()

def text_sample():
    """
    每个关键词每年sample 1000条文本
    """
    SAMPLE_TEXT_DIR = "keyword_text_sample_data"
    if not os.path.exists(SAMPLE_TEXT_DIR):
        os.makedirs(SAMPLE_TEXT_DIR)
    start_date = datetime(2018, 6, 1)
    end_date = datetime(2018, 6, 30)
    date_range = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]
    for keyword in ["努力", "暖心"]: # 
        keyword_sample = []
        for current_date in date_range:
            date_str = current_date.strftime("%Y-%m-%d")
            file_path = f"{TEXT_DIR}/{date_str}.parquet"
            if not os.path.exists(file_path):
                continue
            data = pd.read_parquet(file_path, engine="fastparquet")

            keyword_data = data[data["weibo_content"].str.contains(keyword)]
            sample = keyword_data.sample(300)
            keyword_sample.extend(list(sample["weibo_content"]))
        with open(f"{SAMPLE_TEXT_DIR}/sample_text_{keyword}.txt", "a") as f:
            f.write(f"{keyword}:\n")
            for text in keyword_sample:
                f.write(f"{text}\n")
            f.write("\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", type=str, help="mode to run", default="year", choices=["year", "all", "agg", "sample", "preprocess", "streamline"])
    parser.add_argument("--year", type=int, help="year to analyze", default=2021)
    args = parser.parse_args()
    if args.mode == "year":
        year_analysis(args.year)
    elif args.mode == "all":
        for year in range(2016, 2024):
            year_analysis(year)
    elif args.mode == "agg":
        aggregate()
    elif args.mode == "sample":
        text_sample()
    elif args.mode == "preprocess":
        data_preprocess()
    elif args.mode == "streamline":
        data_preprocess()
        for year in range(2016, 2024):
            year_analysis(year)
        aggregate()

