import os

SOURCE_DIR = "keyword_data"

for kid in range(1, 11):
    # 将year-kid.txt中合并到kid.txt中
    for year in range(2020, 2024):
        if os.path.exists(f"{SOURCE_DIR}/{year}-{kid}.txt"):
            with open(f"{SOURCE_DIR}/{year}-{kid}.txt", 'r') as f:
                lines = f.readlines()
            with open(f"{SOURCE_DIR}/{kid}.txt", 'a') as f:
                f.writelines(lines)
        # os.remove(f"{SOURCE_DIR}/{year}-{kid}.txt")
    