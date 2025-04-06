"""
1. 遍历所有parquet文件，提取高频词
2. 使用bert模型，对于文本进行聚类
"""


import os
import glob
import re
import fire

import jieba
import jieba.analyse

import pandas as pd
import numpy as np

import hdbscan

import torch
from torch.utils.data import Dataset, DataLoader
from transformers import AutoTokenizer, AutoModel
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score, calinski_harabasz_score

from typing import List, Optional, Dict
from collections import defaultdict

# 配置常量
TEXT_DIR = "text_data"
OUTPUT_DIR = "clustering_results"
os.makedirs(OUTPUT_DIR, exist_ok=True)

os.environ["HF_HUB_OFFLINE"] = "1"

# 初始化BERT模型（使用轻量版提高效率）
BERT_MODEL_NAME = "hfl/chinese-roberta-wwm-ext-large"

class TextDataset(Dataset):
    def __init__(self, text_list):
        self.text_list = text_list

    def __len__(self):
        return len(self.text_list)

    def __getitem__(self, idx):
        return self.text_list[idx]
    

class WeiboProcessor(object):
    def __init__(self, 
                 stopwords_file: str = "stopwords.txt"):
        """
        初始化处理器
        :param stopwords_file: 停用词文件路径
        """
        self.parenting_keywords = None
        self.stopwords = self._load_stopwords(stopwords_file)
        

    def _init_jieba(self):
        """初始化jieba配置"""
        jieba.initialize()
        # 添加微博特殊词汇
        jieba.add_word('鸡娃', freq=2000)
        jieba.add_word('双减', freq=2000)
        jieba.suggest_freq(('亲子', '教育'), True)
    

    def _init_bert(self):
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.tokenizer = AutoTokenizer.from_pretrained(BERT_MODEL_NAME)
        self.model = AutoModel.from_pretrained(BERT_MODEL_NAME)
        self.model.eval()
        self.model.to(self.device)

    def _load_stopwords(self, filepath: str) -> set:
        """加载停用词表"""
        if not os.path.exists(filepath):
            return set()
        with open(filepath, 'r', encoding='utf-8') as f:
            return {line.strip() for line in f}

    def clean_weibo_text(self, text: str) -> str:
        """清洗微博文本"""
        if not isinstance(text, str):
            return ""
        # 移除URL、@用户、话题标签等
        text = re.sub(r'(https?://\S+|@\w+|#\w+#)', '', text)
        # 移除特殊符号但保留中文标点
        text = re.sub(r'[^\w\u4e00-\u9fff\，\。\！\？]', '', text)
        return text.strip()

    def tokenize_with_filter(self, text: str) -> List[str]:
        """
        分词并过滤
        :return: 保留的词列表
        """
        text = self.clean_weibo_text(text)
        words = []
        for word in jieba.cut(text):
            word = word.strip()
            # 长度过滤+停用词过滤+词性过滤(可选)
            if (len(word) > 1 and 
                word not in self.stopwords and
                (self.parenting_keywords is None or word in self.parenting_keywords)):
                words.append(word)
        return words

    def process_parquet_files(self, parquet_files: List[str]) -> Dict[str, int]:
        """
        处理多个parquet文件并返回词频统计
        :return: {word: count} 字典
        """
        self._init_jieba()
        word_counts = defaultdict(int)
        for file in parquet_files:
            try:
                df = pd.read_parquet(file)
                for text in df['cleaned_weibo_content']:
                    words = self.tokenize_with_filter(str(text))
                    for word in words:
                        word_counts[word] += 1
            except Exception as e:
                print(f"Error processing {file}: {str(e)}")
        return word_counts

    def save_top_words(self, word_counts: Dict[str, int], output_file: str, top_percent: float = 0.1):
        """
        保存前10%的高频词到CSV
        """
        sorted_words = sorted(word_counts.items(), key=lambda x: -x[1])
        top_n = int(len(sorted_words) * top_percent)
        top_words = sorted_words[:max(top_n, 1)]  # 至少保留1个
        
        df = pd.DataFrame(top_words, columns=['word', 'count'])
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"Saved top {len(df)} words to {output_file}")

    def get_bert_embeddings(self, texts):
        dataset = TextDataset(texts)
        dataloader = DataLoader(dataset, batch_size=512, shuffle=False)
        embeddings = []
        for batch in dataloader:
            inputs = self.tokenizer(
                texts, 
                padding=True, 
                truncation=True, 
                max_length=128, 
                return_tensors="pt"
            )
            with torch.no_grad():
                outputs = self.model(**inputs)
            batch_embeddings = outputs.last_hidden_state[:, 0, :].cpu().numpy()
            embeddings.append(batch_embeddings)
        return np.vstack(embeddings)
    
    def bert_clustering(self, texts: List[str], n_clusters: int = 20) -> np.ndarray:
        """
        使用BERT进行文本聚类
        :return: 聚类标签数组
        """
        self._init_bert()

        text_embeddings = self.get_bert_embeddings(texts)

        clusterer = hdbscan.HDBSCAN(
            min_cluster_size=10,
            metric='euclidean',
            cluster_selection_method='eom',
            prediction_data=True
        )
        cluster_labels = clusterer.fit_predict(text_embeddings)
        unique_labels = set(cluster_labels) - {-1}

        if len(unique_labels) < 2:
            print(f"无法计算轮廓系数")
        else:
            sh_score = silhouette_score(text_embeddings, cluster_labels)
            print(f"Silhouette Score: {sh_score:.4f}")

            overall_ch_score = calinski_harabasz_score(text_embeddings, cluster_labels)
            print(f"Calinski-Harabasz Score: {overall_ch_score:.4f}")

        # 存储结果，每个聚类的文本一个file
        for label in unique_labels:
            cluster_texts = [text for i, text in enumerate(texts) if cluster_labels[i] == label]
            output_file = os.path.join(OUTPUT_DIR, f"cluster_{label}.txt")
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write("\n".join(cluster_texts))
            print(f"Cluster {label} saved to {output_file}")
    
    def cluster_all_parquet_files(self, parquet_files: List[str]):
        """
        对所有parquet文件进行聚类
        :return: None
        """
        all_texts = []
        for file in parquet_files:
            try:
                df = pd.read_parquet(file)
                all_texts.extend(df['cleaned_weibo_content'].dropna().tolist())
            except Exception as e:
                print(f"Error processing {file}: {str(e)}")
        
        self.bert_clustering(all_texts)


def keyword_frequency_extractor(parquet_files, year: Optional[int] = None):

    
    # 2. 初始化处理器
    processor = WeiboProcessor()
    
    # 3. 处理文件并统计词频
    word_counts = processor.process_parquet_files(parquet_files)
    
    # 4. 保存结果
    output_file = os.path.join(OUTPUT_DIR, f"top_words_{year or 'all'}.csv")
    processor.save_top_words(word_counts, output_file)

def clustering(parquet_files, year: Optional[int] = None):
    processor = WeiboProcessor()
    processor.cluster_all_parquet_files(parquet_files)




def main(action: str, year: Optional[int] = None):
    """
    主处理函数
    :param action: 行动，frequency/clustering
    :param year: 指定处理的年份，None表示处理所有年份
    """

    # 1. 定位文件
    parquet_files = []
    if year is not None:
        pattern = f"{TEXT_DIR}/{year}-*.parquet"
    else:
        pattern = f"{TEXT_DIR}/*.parquet"
    parquet_files = glob.glob(pattern)
    
    if not parquet_files:
        print(f"No parquet files found with pattern: {pattern}")
        return
    
    if action == "frequency":
        keyword_frequency_extractor(parquet_files, year)
    elif action == "clustering":
        clustering(parquet_files, year)

if __name__ == "__main__":
    fire.Fire(main)