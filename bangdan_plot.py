"""
在bangdan_working_data中存储了2019-2023年的榜单数据
每个月一个文件（e.g. 2020-01.csv）
文件内容没有标题行
单行格式为timestamp,date,text,hot,rear
需要处理为每个月rear为1的proportion，绘制一个趋势图
"""
import os

from collections import defaultdict
from datetime import datetime

from matplotlib import pyplot as plt
import pandas as pd

import seaborn as sns


# 预处理，处理文件中的格式问题
# for year in ["2019", "2020", "2021", "2022", "2023"]:
#     for month in range(1, 13):
#         month_str = f"{year}-{month:02d}"
#         file_path = f"bangdan_working_data/{month_str}.csv"
#         if not os.path.exists(file_path):
#             continue
#         print(f"cleaning {file_path}")

#         # 遍历每一行，如果这一行用,split之后的长度不是5，则需要做处理（这一行是两行，多于5个元素是忘记换行的结果）
#         result = []
#         with open(file_path, "r") as rfile:
#             for line in rfile:
#                 line = line.strip()
#                 if len(line.split(",")) != 5:
#                     if len(line.split(",")) == 9:
#                         # 这一行是两行，多于5个元素是忘记换行的结果, 第五个元素的第一个数字是上一行的最后一个元素，后面是下一行的第一个元素
#                         line_data = line.split(",")
#                         new_line_data = line_data[:5]
#                         new_line_data[4] = new_line_data[4][0]
#                         result.append(",".join(new_line_data))
#                         new_line_data = line_data[4:]
#                         new_line_data[0] = new_line_data[0][1:]
#                         result.append(",".join(new_line_data))
#                 else:
#                     result.append(line)
        
#         with open(file_path, "w") as wfile:
#             wfile.write("\n".join(result))
#             wfile.write("\n")

data_mode = "_strict2"

def run(mode, cutoff, timewindow):
    month_rear_proportion = defaultdict(float)

    month_rear_and_no_rear_data = {
        "month": [],
        "rear": [],
        "total": []
    }

    season_data = defaultdict(lambda: defaultdict(int))
    year_data = defaultdict(lambda: defaultdict(int))

    total_data = 0
    with_hot_data = 0

    for year in ["2019", "2020", "2021", "2022", "2023"]:
        for month in range(1, 13):
            month_str = f"{year}-{month:02d}"
            
            season_map = {
                0: "1-3",
                1: "4-6",
                2: "7-9",
                3: "10-12"
            }
            season = season_map[(month - 1) // 3]

            file_path = f"bangdan_working_data/{month_str}{data_mode}.csv"
            if not os.path.exists(file_path):
                continue
            print(f"processing {file_path}")

            # 用pandas读取df，手动指定列明
            df = pd.read_csv(file_path)
            
            # total data记录数据总行数
            total_data += len(df)
            
            if mode == "weighted":
                df.dropna(subset=["hot"], how="any", inplace=True)
                df["hot"] = df["hot"].astype(int)
                # with hot data记录有hot数据的行数
                with_hot_data += len(df)

                df = df.loc[df.groupby('text')['hot'].idxmax()]
            if cutoff is not None:
                df.dropna(subset=["hot"], how="any", inplace=True)
                df["hot"] = df["hot"].astype(int)
                df = df[df["hot"] >= cutoff]

            rear_count = df["rear"].sum()
            total_count = len(df)

            if mode == "weighted":
                # 用hot的值作为权重, rear count为rear为1的行的hot的和，totalcount为hot的和
                df["rear"] = df["rear"].astype(int)
                # rear count为rear为1的行的hot的和
                rear_count = (df["rear"] * df["hot"]).sum()
                # totalcount为hot的和
                total_count = df["hot"].sum()

            if timewindow == "month":
                if year == "2020" and month in [9, 10]:
                    # 去除异常低值
                    continue
                month_rear_proportion[month_str] = rear_count / total_count if total_count > 0 else 0

                month_rear_and_no_rear_data["month"].append(month_str)
                month_rear_and_no_rear_data["rear"].append(rear_count)
                month_rear_and_no_rear_data["total"].append(total_count)
            elif timewindow == "season":
                if year == "2023" and season == "10-12":
                    continue
                season_data[f"{year}-{season}"]["rear"] += rear_count
                season_data[f"{year}-{season}"]["total"] += total_count
            elif timewindow == "year":
                year_data[year]["rear"] += rear_count
                year_data[year]["total"] += total_count

    if timewindow == "season":
        for season, data in season_data.items():
            month_rear_proportion[season] = data["rear"] / data["total"] if data["total"] > 0 else 0
            month_rear_and_no_rear_data["month"].append(season)
            month_rear_and_no_rear_data["rear"].append(data["rear"])
            month_rear_and_no_rear_data["total"].append(data["total"])
                
    elif timewindow == "year":
        for year, data in year_data.items():
            month_rear_proportion[year] = data["rear"] / data["total"] if data["total"] > 0 else 0
            month_rear_and_no_rear_data["month"].append(year)
            month_rear_and_no_rear_data["rear"].append(data["rear"])
            month_rear_and_no_rear_data["total"].append(data["total"])
    
    
    # 用一个dataframe存储proportion数据
    month_rear_proportion = pd.DataFrame.from_dict(month_rear_proportion, orient="index", columns=["proportion"])
    # 折线图-proportion的变化
    sns.set(style="ticks")

    plt.figure(figsize=(10, 6))
    sns.lineplot(data=month_rear_proportion, marker="o", linewidth=4, alpha=1)
    # sns.regplot(data=month_rear_proportion, scatter=False, color=sns.color_palette()[0], line_kws={"linestyle": "--", "linewidth": 2, "alpha": 0.3}, ci=None)

    plt.title("Proportion of Childrearing-Related Trending Topics on Weibo (2019–2023)", fontsize=14)
    plt.xlabel(f"{timewindow}")
    plt.ylabel("Proportion of Childrearing-Related Topics")
    plt.xticks(rotation=45)
    plt.tight_layout()

    if os.path.exists(f"img{data_mode}") is False:
        os.mkdir(f"img{data_mode}")

    plt.savefig(f"img{data_mode}/rear_proportion_trend_{mode}_{timewindow}_{cutoff if cutoff is not None else 'nocutoff'}.pdf", format="pdf")


    # 用同一个坐标中的两个面积图展示rear和no rear的数量
    # plt.figure(figsize=(10, 6))
    # plt.stackplot(month_rear_and_no_rear_data["month"], month_rear_and_no_rear_data["rear"], month_rear_and_no_rear_data["total"])
    # plt.xticks(rotation=45)
    # plt.title("Rear and No Rear Trend")
    # plt.xlabel(f"{timewindow}")
    # plt.ylabel("Count" if mode == "additive" else "Weighted Hot Score")
    # plt.legend(["Rear", "No Rear"])
    # plt.grid()
    # plt.tight_layout()
    # plt.savefig(f"img{data_mode}/rear_and_no_rear_trend_{mode}_{timewindow}_{cutoff if cutoff is not None else 'nocutoff'}.pdf", format="pdf")

    # 打印hot缺失值比例
    print(f"hot缺失值比例: {1 - with_hot_data / total_data}")


if __name__ == "__main__":
    mode = ["additive", "weighted"]
    # mode = ["weighted"]
    cutoff = [None]
    # cutoff = [None, 100000, 500000]
    timewindow = ["month", "season", "year"]
    timewindow = ["year"]
    for m in mode:
        for c in cutoff:
            for t in timewindow:
                run(m, c, t)
