import pandas as pd
import os


for year in ["2019", "2020", "2021", "2022", "2023"]:
    all_rear_df = []
    total_num = 0
    rear_num = 0
    for month in range(1, 13):
        file_path = f"bangdan_working_data/{year}-{month:02d}_strict2.csv"
        if not os.path.exists(file_path):
            continue

        df = pd.read_csv(file_path)
        df["rear"] = df["rear"].astype(int)
        total_num += len(df)
        df = df[df["rear"] == 1]
        rear_num += len(df)
        
        df.dropna(subset=["hot"], how="any", inplace=True)
        df["hot"] = df["hot"].astype(int)
        df = df.loc[df.groupby('text')['hot'].idxmax()]
        all_rear_df.append(df)


    rear_df = pd.concat(all_rear_df)
    rear_df.to_csv(f"rear_{year}.csv", index=False)
    print(f"Year {year}: rear-{rear_num}, total-{total_num}, proportion-{rear_num/total_num}")

