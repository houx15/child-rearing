"""
original_keywords.xlsx中有品质、近义词、反义词三列
我需要输出的是：
1. 给每个品质词一个keyword id
输出一个dict key是keyword 的id
value:
{
    "keyword": "品质",
    "synonyms": ["质量", "素质"],
    "antonyms": ["劣质"]
}
"""

import pandas as pd

def get_keywords_dict():
    keywords = pd.read_excel('data/original_keywords.xlsx')
    keywords_dict = {}
    for i in range(len(keywords)):
        kid = 1 + i
        keyword = keywords.iloc[i, 0]
        synonyms = keywords.iloc[i, 1].split('、')
        antonyms = keywords.iloc[i, 2].split('、')
        keywords_dict[str(int(kid))] = {
            "keyword": keyword,
            "synonyms": synonyms,
            "antonyms": antonyms
        }
    return keywords_dict

keyword_dict = get_keywords_dict()

import json
with open('data/keywords_dict.json', 'w') as f:
    json.dump(keyword_dict, f, ensure_ascii=False)