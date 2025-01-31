import pandas as pd

# 尝试加载text_data/2020-01-01.parquet, print shape和前10行
for i in range(31):
    date = i+1
    df = pd.read_parquet(f"text_data/2020-01-{str(date).zfill(2)}.parquet")
    if df.shape[0] == 0:
        continue
    print(df.shape)
    print(df.head(10))
    break