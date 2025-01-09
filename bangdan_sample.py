import os

import pandas as pd

all_rear_df = []
sampled_rear_df = []

for year in ["2019", "2020", "2021", "2022", "2023"]:
    for month in range(1, 13):
        month_str = f"{year}-{month:02d}"

        file_path = f"bangdan_working_data/{month_str}.csv"
        if not os.path.exists(file_path):
            continue
        print(f"processing {file_path}")

        # 用pandas读取df，手动指定列明
        df = pd.read_csv(file_path, header=None, names=["timestamp", "date", "text", "hot", "rear"])
        df["rear"] = df["rear"].astype(int)
        df = df[df["rear"] == 1]

        # 去除hot缺失的行
        df.dropna(subset=["hot"], how="any", inplace=True)
        df["hot"] = df["hot"].astype(int)

        #根据text列进行去重，保留hot最大的一行
        df = df.loc[df.groupby('text')['hot'].idxmax()]

        all_rear_df.append(df)

        # 从df中随机抽取10条数据，考虑到不足10的情况
        sampled_df = df.sample(n=10, replace=True) if len(df) >= 10 else df.sample(n=len(df), replace=True)
        sampled_rear_df.append(sampled_df)


all_rear_df = pd.concat(all_rear_df)
sampled_rear_df = pd.concat(sampled_rear_df)

# print length
print(len(all_rear_df))
print(len(sampled_rear_df))

all_rear_df.to_csv("all_rear_data.csv", index=False)
sampled_rear_df.to_csv("sampled_rear_data.csv", index=False)