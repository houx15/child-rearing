import os
from collections import defaultdict
import json

SOURCE_DIR = "keyword_data"

for kid in range(1, 11):
    # 将year-kid.txt中合并到kid.txt中
    if os.path.exists(f"{SOURCE_DIR}/{kid}.txt"):
        os.remove(f"{SOURCE_DIR}/{kid}.txt")

    for year in range(2020, 2024):
        if os.path.exists(f"{SOURCE_DIR}/{year}-{kid}.txt"):
            with open(f"{SOURCE_DIR}/{year}-{kid}.txt", 'r') as f:
                lines = f.readlines()
            with open(f"{SOURCE_DIR}/{kid}.txt", 'a') as f:
                f.writelines(lines)
        
        

all_keyword_count = defaultdict(int)

for year in range(2020, 2024):
    with open(f"{SOURCE_DIR}/{year}-keywords_count.json", 'r') as f:
        keyword_count = json.load(f)
    
    for k, v in keyword_count.items():
        all_keyword_count[k] += v

with open(f"{SOURCE_DIR}/all_keywords_count.json", 'w') as f:
    json.dump(all_keyword_count, f, ensure_ascii=False)