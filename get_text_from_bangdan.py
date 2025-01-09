"""
处理文本数据
整体思路：
【第一步】
- 生成两个映射：1. 话题-日期； 2. 日期-话题列表（作为待检索的关键词）

【第二步】
- 逐日处理数据：
  - 解压缩数据到text_working_data
  - 分块读取数据（单次处理50w条，多进程-4个进程）
  - 使用aho-corasick匹配文本（模式：#关键词#），如果匹配成功，则对这一行进行处理，保留其中的id、timestamp、文本信息，保留方式为存储在内存的一个dict中，dict的key是关键词的id，value是一个list，list的单个元素是上述信息

"""

import os
import json
import time

import pandas as pd

import ahocorasick
from collections import defaultdict
from datetime import datetime, timedelta
from concurrent.futures import ProcessPoolExecutor

from configs.configs import *
from utils.utils import *


TEXT_DIR = "text_data"

def log(text, lid=None):
    output = f"logs/log_{lid}.txt" if lid is not None else "logs/log.txt"
    with open(output, "a") as f:
        f.write(f"{text}\n")


def get_zipped_fresh_data_file(year, date):
    """
    date should be yyyy-mm-dd format
    """
    return f"{DATA_SOURCE_DIR}/{year}/freshdata/weibo_freshdata.{date}.7z"


def get_unzipped_fresh_data_folder(year):
    return f"text_working_data/{year}/"


def get_unzipped_fresh_data_file(year, date):
    if date == "2020-06-30":
        return f"text_working_data/{year}/weibo_2020-06-30.csv"
    elif date in ["2017-01-11", "2016-07-24", "2016-08-09"]:
        return f"text_working_data/{year}/weibo_log/weibo_freshdata.{date}.csv"
    return f"text_working_data/{year}/weibo_freshdata.{date}"


def delete_unzipped_fresh_data_file(year, date):
    """
    处理完毕之后需要删除文件
    """
    file_path = get_unzipped_fresh_data_file(year, date)
    # 删除文件
    try:
        os.remove(file_path)
        print(f"文件 {file_path} 已成功删除。")
    except FileNotFoundError:
        print(f"文件 {file_path} 不存在。")
    except Exception as e:
        print(f"删除文件时发生错误: {e}")


def unzip_one_fresh_data_file(year, date):
    """
    date should be yyyy-mm-dd format
    """
    unzipped_file_path = get_unzipped_fresh_data_file(year, date)

    # 检查文件是否已经解压
    if os.path.exists(unzipped_file_path):
        print(f"文件 {unzipped_file_path} 已经存在，跳过解压。")
        return unzipped_file_path

    # 如果文件不存在，则进行解压
    zipped_file_path = get_zipped_fresh_data_file(year, date)
    unzipped_dir = get_unzipped_fresh_data_folder(year)
    result = extract_single_7z_file(
        file_path=zipped_file_path, target_folder=unzipped_dir
    )

    if os.path.exists(unzipped_file_path):
        print(f"文件 {unzipped_file_path} 解压成功。")
        return unzipped_file_path
    else:
        print(f"文件 {unzipped_file_path} 解压失败。")
        return None



def process_chunk_special(chunk, automation, result_set):
    for line in chunk:
        for end_index, (kid, keyword) in automation.iter(line):
            """
            "46890032291","2020-06-30 00:12:36","1593447156000","1","2789934082","妞子蓝楸瑛","https://tva1.sinaimg.cn/crop.0.0.180.180.50/a64b0402jw1e8qgp5bmzyj2050050aa8.jpg?KID=imgbed,tva&Expires=1593457954&ssig=WuAFhSJ49R","普通用户","4520977143508623","转发微博","0","0","0","J8NI6eIKH","微博 weibo.com","","2020-06-29 02:20:09","1593368409","2920534890","地盘鲁路修兰佩洛基1986","普通用户","4247252011050572","双子座 今日(6月4日)综合运势：5，幸运颜色：粉色，幸运数字：7，速配星座：天蝎座（分享自@微心情） 查看更多：http://t.cn/h5gw6 ​​​","95","0","0","GjPeV4piI","微博 weibo.com","","2018-06-04 18:14:11","1528107251","","0","","0","0","0","0","2020-06-30"
            """
            line_data = line.strip().strip('"').split('","')
            try:
                weibo_content = line_data[9].replace('\n', ' ') if line_data[3] == "0" else line_data[9].replace('\n', ' ') + '//' + line_data[22].replace('\n', ' ')
                result_set.add([kid,line_data[0],line_data[4],line_data[2],line_data[3],line_data[10],line_data[11],line_data[12],weibo_content])
            except:
                continue

def process_chunk(chunk, automaton, result_set):
    for line in chunk:
        for end_index, (kid, keyword) in automaton.iter(line):
            """
            40984940671        {"id":"40984940671","crawler_time":"2020-01-01 04:27:59","crawler_time_stamp":"1577824079000","is_retweet":"0","user_id":"5706021763","nick_name":"诗词歌赋","tou_xiang":"https:\/\/tvax2.sinaimg.cn\/crop.0.0.1002.1002.50\/006e9SV5ly8g4yg7ozexlj30ru0ruabp.jpg?KID=imgbed,tva&Expires=1577834878&ssig=lHvYHGBxwq","user_type":"黄V","weibo_id":"4455589780114474","weibo_content":"给自己设立一个目标，给自己未来一个明确的希望，给自己的生活一个方向灯。冲着这个方向而努力，不断去超越自己，提高自己的水平，不能让自己有懈怠的时候。早安! ","zhuan":"0","ping":"0","zhan":"0","url":"Ink8W0tMm","device":"Redmi Note 7 Pro","locate":"","time":"2019-12-31 15:54:07","time_stamp":"1577778847","r_user_id":"","r_nick_name":"","r_user_type":"","r_weibo_id":"","r_weibo_content":"","r_zhuan":"","r_ping":"","r_zhan":"","r_url":"","r_device":"","r_location":"","r_time":"","r_time_stamp":"","pic_content":"","src":"4","tag":"106750860151","vedio":"0","vedio_image":"","edited":"0","r_edited":"","isLongText":"0","r_isLongText":"","lat":"","lon":"","d":"2020-01-01"}
            """
            line_data = line.strip().split("\t")
            try:
                data = json.loads(line_data[1])
            except IndexError as e:
                print(f"IndexError occurred: {e}")
                continue
            except json.JSONDecodeError as e:
                print(f"JSONDecodeError: {e}")
                # 打印出错误位置
                print(f"Error at line {e.lineno}, column {e.colno}")
                # 打印出错误字符位置
                print(f"Error at character {e.pos}, {line_data[1][int(e.pos)-20: int(e.pos)+20]}")
                continue
            
            try:
                weibo_content = data['weibo_content'].replace('\n', ' ') if data['is_retweet'] == "0" else data['weibo_content'].replace('\n', ' ') + '//' + data['r_weibo_content'].replace('\n', ' ')
                result_set.add([kid,data['weibo_id'],data['user_id'],data['time_stamp'],data['is_retweet'],data['zhuan'],data['ping'],data['zhan'],weibo_content])
            except KeyError:
                continue



def process_file(file_path, keywords):
    """
    处理单个文件并完成存储
    """
    # 初始化 Aho-Corasick 自动机
    automaton = ahocorasick.Automaton()
    for idx, keyword in keywords.items():
        # 添加关键词，假设关键词格式为 #keyword#
        automaton.add_word(f"{keyword}", (idx, keyword))
    automaton.make_automaton()

    # 结果字典
    result_set = set()

    # 读取文件并分块处理
    chunk_size = 500000
    with open(file_path, 'r', encoding='utf-8', errors='replace') as file:
        chunk = []
        for line in file:
            chunk.append(line.strip())
            if len(chunk) == chunk_size:
                process_chunk(chunk, automaton, result_set)
                chunk = []
        # 处理最后一个不满 chunk_size 的块
        if chunk:
            process_chunk(chunk, automaton, result_set)
    
    return result_set


def append_to_parquet(date, results):
    """
    将数据追加到指定年份的 Parquet 文件中。
    :param year: 年份（如 2024）
    :param userids: 用户 ID 列表
    :param results: 结果数据列表
    """
    output_parquet_path = f"{TEXT_DIR}/{date}.parquet"
    columns = ["keyword_id", "weibo_id", "user_id", "time_stamp", "is_retweet", "zhuan", "ping", "zhan", "weibo_content"]
    df = pd.DataFrame(list(results), columns=columns)

    # # 检查文件是否存在
    # if not os.path.exists(output_parquet_path):
    #     # 如果文件不存在，直接写入（新建文件）
    df.to_parquet(output_parquet_path, engine="fastparquet", index=False)
    # else:
    #     # 如果文件存在，以追加模式写入
    #     df.to_parquet(output_parquet_path, engine="fastparquet", index=False, append=True)
    #     log(f"追加数据到文件：{output_parquet_path}")


def process_year(year, mode):
    rear_bangdan = pd.read_csv(f"rear_{year}.csv")
    # 给rear_bangdan生成一个id列，生成规则为2019-独立id
    rear_bangdan['id'] = f"{year}-{rear_bangdan.index + 1}"

    # rear_bangdan有date列：yyyy-mm-dd 我希望后面能够按照月份筛选出某个月份的全部行
    rear_bangdan['date'] = pd.to_datetime(rear_bangdan['date'])

    start_date_options = [datetime(year, 1, 1), datetime(year, 7, 1)]
    end_date_options = [datetime(year, 6, 30), datetime(year, 12, 31)]
    start_date = start_date_options[mode]
    end_date = end_date_options[mode]

    current_date = start_date

    date_range = [start_date + timedelta(days=n) for n in range((end_date - start_date).days + 1)]
    
    for current_date in date_range:
        date_str = current_date.strftime("%Y-%m-%d")

        # 按照date所属月份，筛选出rear_bangdan中的行, key是id，value是keyword
        keywords = rear_bangdan[rear_bangdan['date'].dt.month == current_date.month].set_index('id')['keyword'].to_dict()
        # 如果没有元素，跳过
        if not keywords:
            continue
        
        file_path = unzip_one_fresh_data_file(year, date_str)
        # file_path = f"text_working_data/{year}/weibo_freshdata.test"
        if file_path is None:
            continue
        start_timestamp = int(time.time())
        results = process_file(file_path, keywords)
        append_to_parquet(date_str, results)

        log(
            f"处理 {date_str} 完成，耗时 {int(time.time()) - start_timestamp} 秒。",
            f"{year}_{mode}",
        )

        delete_unzipped_fresh_data_file(year, date_str)
        print(f"finished {date_str} with {len(results)} records")


if __name__ == "__main__":
    # for y in [2016, 2017, 2018, 2019]:
    process_year(2020, 1)
    log("处理完毕。")
