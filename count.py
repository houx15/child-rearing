import pandas as pd
import glob
import os
import re
from pathlib import Path
from collections import defaultdict


def count_lines(target_dir):
    """
    针对该dir下所有形如 yyyy-mm-dd.parquet 的文件，统计其行数

    Args:
        target_dir: 目标目录路径

    Returns:
        dict: 包含每个文件的行数统计，格式为 {日期: 行数}
        dict: 按年份汇总的统计，格式为 {年份: 总行数}
    """
    file_lines = {}
    year_lines = defaultdict(int)

    # 使用 glob 匹配所有 parquet 文件
    pattern = os.path.join(target_dir, "*.parquet")
    parquet_files = glob.glob(pattern)

    if not parquet_files:
        print(f"警告: 在目录 {target_dir} 中未找到任何 .parquet 文件")
        return {}, {}

    # 定义文件名格式：yyyy-mm-dd.parquet
    date_pattern = re.compile(r"^\d{4}-\d{2}-\d{2}$")

    print(f"找到 {len(parquet_files)} 个 parquet 文件，开始筛选和统计...")

    matched_count = 0
    for file_path in sorted(parquet_files):
        # 提取文件名（不含扩展名）
        filename = Path(file_path).stem

        # 只处理符合 yyyy-mm-dd 格式的文件
        if not date_pattern.match(filename):
            continue

        matched_count += 1
        try:
            # 读取 parquet 文件
            df = pd.read_parquet(file_path)
            line_count = len(df)

            file_lines[filename] = line_count

            # 提取年份并汇总
            year = filename[:4]
            year_lines[year] += line_count

        except Exception as e:
            print(f"错误: 读取文件 {file_path} 时出错: {e}")
            continue

    if matched_count == 0:
        print(
            f"警告: 在目录 {target_dir} 中未找到任何符合 yyyy-mm-dd.parquet 格式的文件"
        )
        return {}, {}

    print(f"筛选出 {matched_count} 个符合格式的文件")

    return file_lines, dict(year_lines)


if __name__ == "__main__":
    target_directory = "text_data"

    if not os.path.exists(target_directory):
        print(f"错误: 目录 {target_directory} 不存在")
    else:
        file_lines, year_lines = count_lines(target_directory)

        print("\n=== 按文件统计 ===")
        for filename, count in sorted(file_lines.items()):
            print(f"{filename}: {count:,} 行")

        print("\n=== 按年份汇总 ===")
        for year, count in sorted(year_lines.items()):
            print(f"{year}年: {count:,} 行")

        print(f"\n总计: {sum(file_lines.values()):,} 行")
