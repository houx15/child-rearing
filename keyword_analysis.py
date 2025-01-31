"""
关键词词频分析
在source folder中有1-10.txt，每个文件中存储了一个关键词的微博内容
需要对每个关键词的微博内容进行词频统计
输出一个dict，key是关键词，value是词频统计结果

我希望去除停用词
最好只保留长度为2-4的形容词、名词、动词
"""

import os
import jieba
import json
from utils.utils import weibo_text_cleaner
from collections import defaultdict

SOURCE_DIR = "keyword_data"

def get_keywords_dict():
    """加载关键词字典并预处理"""
    with open('data/keywords_dict.json', 'r') as f:
        keywords_dict = json.load(f)
    for k, v in keywords_dict.items():
        # 将同义词和反义词合并为集合，便于快速匹配
        keywords_dict[k]['all_keywords'] = set(v['synonyms'] + v['antonyms'])
    return keywords_dict


def get_stopwords():
    with open('data/stopwords.txt', 'r') as f:
        stopwords = set(f.read().splitlines())
    return stopwords

def get_word_freq(content, stopwords):
    words = jieba.cut(content,)
    word_freq = {}
    for word in words:
        if word in stopwords:
            continue
        if 2 <= len(word) <= 4:
            word_freq[word] = word_freq.get(word, 0) + 1
    return word_freq

def get_word_freq_dict():
    """
    每个kid写入一个csv，命名为kid.csv，第一列为word，第二列为词频。保留词频大于总行数10%的词，按词频降序排列
    """
    keywords_dict = get_keywords_dict()

    stopwords = get_stopwords()
    for kid in range(1, 11):
        kname = keywords_dict[str(int(kid))]['keyword']

        with open(f"{SOURCE_DIR}/{kid}.txt", 'r') as f:
            content = f.read()
        cleaned_content = []
        for line in content.splitlines():
            cleaned_line = weibo_text_cleaner(line)
            if cleaned_line:
                cleaned_content.append(cleaned_line)
        word_freq = get_word_freq("\n".join(cleaned_content), stopwords)
        total_lines = len(cleaned_content)
        print(f"Total lines for kid {kid}: {total_lines}")
        word_freq = {k: v for k, v in word_freq.items() if v > total_lines * 0.05}
        word_freq = sorted(word_freq.items(), key=lambda x: x[1], reverse=True)
        with open(f"{SOURCE_DIR}/{kname}-freq.csv", 'w') as f:
            f.write("word,freq\n")
            for word, freq in word_freq:
                f.write(f"{word},{freq}\n")

def output_keyword_count_to_csv():
    keywords_dict = get_keywords_dict()
    synoyms_to_keywords = defaultdict(set)
    for k, v in keywords_dict.items():
        for i in v["all_keywords"]:
            synoyms_to_keywords[i].add(v["keyword"])
    
    for k, v in synoyms_to_keywords.items():
        synoyms_to_keywords[k] = "、".join(list(v))

    with open(f"{SOURCE_DIR}/all_keywords_count.json", 'r') as f:
        all_keyword_count = json.load(f)
    
    with open(f"{SOURCE_DIR}/all_keywords_count.csv", 'w') as f:
        f.write("关键词,所属品质,词频\n")
        for k, v in all_keyword_count.items():
            f.write(f"{k},{synoyms_to_keywords[k]},{v}\n")

if __name__ == '__main__':
    get_word_freq_dict()
    output_keyword_count_to_csv()
