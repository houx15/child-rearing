"""
@ Author: Hou Yuxin
@ Date: November 22, 2024
@ Target: Drag several hot debated topics from bangdan data

Bangdan File Structure
```
|-bangdan_data
|---year
|-----weibo_bangdan.yyyy-mm-dd
```

Bangdan Data Fromat
```
61543(integer)\tjson_data_wrap1\n
```

json_data_wrap1
```
{
    "id": xxxxx,
    "crawler_time": xxxxx,
    "crawler_time_stamp": xxxx,
    "type": 1, # 1-real time, 2-hottest
    "bangdan": json_data_wrap2,
    "date": xxxx
}
```

json_data_wrap2 为节约空间，仅显示相关内容
```
{
    "cards": [], #n个卡片
    "cardlistInfo": {},
    xxx
}
```

cards 仅考虑card_type == 11的内容，是实际展示的榜单
```
{
    "card_type": 11,
    "title": "xxx",
    "show_type": xx,
    "card_group": [
        {
            "card_type": 4,
            "pic": "",
            "desc": "xxx" #需要的就是这个desc
        }
    ],
    "openurl"
}
```

"""

import os
import re
import json
from datetime import datetime, timedelta

from configs.configs import *
from utils.utils import extract_7z_files, REAR_KEYWORDS


def get_bangdan_files_dir(year):
    return f"{DATA_SOURCE_DIR}/{year}/bangdan/"


def get_bangdan_unzipped_files_dir(year):
    return f"bangdan_data/{year}/"


def unzip_all_bangdan_files():
    """
    将原始微博数据解压缩到当前目录的bangdan_data文件夹
    """
    for year in ANALYSIS_YEARS:
        bangdan_files_dir = get_bangdan_files_dir(year)
        unzipped_dir = get_bangdan_unzipped_files_dir(year)
        extract_7z_files(source_folder=bangdan_files_dir, target_folder=unzipped_dir)


class BangdanAnalyzer(object):

    def __init__(
        self,
        year: int,
        
    ):
        self.year = year
        self.data_dir = get_bangdan_unzipped_files_dir(year)
        self.bangdan_type = "1"
        self.rear_pattern = "|".join([f"{''.join([f'(?=.*{word})' for word in keyword.split('&')])}" for keyword in REAR_KEYWORDS])
    
    
    def get_file_path(self, date: str = None):
        # date should be yyyy-mm-dd format
        return os.path.join(self.data_dir, f"weibo_bangdan.{date}")

    def get_bangdan_text_from_file(self, file_path: str, date: str):
        """
        一行bangdan信息的格式：timestamp,date,text,hot,rear
        例如：1111111111,2022-01-01,这是一个热搜话题,10000000,100
        """

        bangdan_text_list = []
        
        # 考虑file path是否存在
        if not os.path.exists(file_path):
            print(f"File not exists: {file_path}")
            return None
        with open(file_path, "r", errors="replace") as rfile:
            for line in rfile.readlines():
                line = line.strip()
                line_data = line.split("\t")
                if len(line_data) < 2:
                    print("line data cannot be splitted")
                    continue
                try:
                    data = json.loads(line_data[1])
                except json.JSONDecodeError as e:
                    print(f"JSONDecodeError: {e}")
                    # 打印出错误位置
                    print(f"Error at line {e.lineno}, column {e.colno}")
                    # 打印出错误字符位置
                    print(f"Error at character {e.pos}, {line_data[1][int(e.pos)-20: int(e.pos)+20]}")
                    continue
                crawler_time_stamp = data["crawler_time_stamp"]
                if data["type"] != self.bangdan_type:
                    # print(f"wrong data type: {data['type']}")
                    # 排除不允许的榜单类型
                    # 不是实时榜
                    continue
                data = json.loads(data["bangdan"])
                if type(data) is not dict:
                    print(f"bad data type")
                    print(data)
                    continue
                if "cards" not in data.keys() or data["cards"] is None:
                    print(f"bad data type in file {file_path}")
                    continue
                for card in data["cards"]:
                    if str(card["card_type"]) != "11":
                        continue
                    card_group = card["card_group"]
                    for s_card in card_group:
                        if str(s_card["card_type"]) != "4":
                            continue
                        if "desc" in s_card.keys():
                            text = s_card["desc"]
                            if len(text) <= 5:
                                # 太短的话题丢掉
                                continue

                            hot = ""
                            if "desc_extr" in s_card.keys():
                            # 讨论小于10w的丢掉
                            # print(s_card["desc_extr"])
                                hot = re.findall(r'\d+', str(s_card["desc_extr"]))
                            
                            is_rear = 1 if re.search(self.rear_pattern, text) is not None else 0
                                

                            bangdan_text_list.append(f"{crawler_time_stamp},{date},{text},{hot[0]},{is_rear}")

                        else:
                            print(f"desc not in keys! file_name {file_path}, data: {s_card}")
        return bangdan_text_list
    


    def analyze(self):
        # 遍历self.year的一整年的每一天 (通过datetime)
        for date in [datetime(self.year, 1, 1) + timedelta(days=i) for i in range(365)]:
            date_str = date.strftime("%Y-%m-%d")
            month_str = date.strftime("%Y-%m")
            file_path = self.get_file_path(date_str)
            if not os.path.exists(file_path):
                print(f"File not exists: {file_path}")
                continue
            bangdan_text_list = self.get_bangdan_text_from_file(file_path, date_str)
            if bangdan_text_list is None:
                continue
            with open(f"bangdan_working_data/{month_str}.csv", "a") as wfile:
                wfile.write("\n".join(bangdan_text_list))
            print(f"processed {date_str} in year {self.year}")


if __name__ == "__main__":
    unzip_all_bangdan_files()
    for year in ANALYSIS_YEARS:
        # if year in [2020, 2021]:
        #     continue
        print(f"\n\nprocessing year-{year}")
        analyzer = BangdanAnalyzer(
            year=year,
        )
        analyzer.analyze()
