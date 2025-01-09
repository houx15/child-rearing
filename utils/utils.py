import os
import re
import py7zr

REAR_KEYWORDS = ["家庭教育", "家长", "育儿", "教育孩子", "培养孩子", "抚养", "穷养", "富养", "管教孩子", "管孩子", "带娃", "带孩子", "养育", "养娃", "养孩子", "教育方式", "挫折教育", "父母", "父亲", "母亲", "爸爸", "妈妈", "老爸", "老妈", "爸妈", "宝爸", "宝妈", "子女", "女儿", "儿子", "女孩", "男孩", "女童", "男童", "孙女", "孙子", "陪读", "孩子&学习", "辅导&作业", "辅导&功课", "孩子&养", "别人家&孩子"]


STRICT_REAR_KEYWORDS = ["家庭教育", "家长", "育儿", "教育孩子", "培养孩子", "穷养", "富养", "管教孩子", "管孩子", "带娃", "带孩子", "养育", "养娃", "养孩子", "教育方式", "挫折教育", "宝爸", "宝妈", "子女", "陪读", "孩子&学习", "辅导&作业", "辅导&功课", "孩子&养", "别人家&孩子"]


def extract_7z_files(source_folder, target_folder):
    # 确保目标文件夹存在
    if not os.path.exists(target_folder):
        os.makedirs(target_folder)

    # 遍历源文件夹中的所有文件
    for file_name in os.listdir(source_folder):
        # 检查文件是否是.7z文件
        if file_name.endswith(".7z"):
            file_path = os.path.join(source_folder, file_name)
            with py7zr.SevenZipFile(file_path, mode="r") as archive:
                # 解压文件到目标文件夹
                archive.extractall(path=target_folder)
                print(f"Extracted: {file_name}")


def extract_single_7z_file(file_path, target_folder):
    # 确保目标文件夹存在
    if not os.path.exists(target_folder):
        os.makedirs(target_folder)

    # 检查文件是否是.7z文件
    try:
        if file_path.endswith(".7z"):
            with py7zr.SevenZipFile(file_path, mode="r") as archive:
                # 解压文件到目标文件夹
                archive.extractall(path=target_folder)
                print(f"Extracted: {file_path}")
                return "success"
    except:
        return None


def weibo_text_cleaner(sentence):
    if len(sentence) < 10:
        return None
    sentence = sentence.replace("点击链接查看更多->", "")
    sentence = sentence.replace("Forward Weibo", "")
    sentence = sentence.replace("快转微博", "")
    sentence = sentence.replace("转发微博", "")
    sentence = sentence.replace("video", "")
    url_pattern = re.compile(r'https?://[^\s]+')
    sentence = re.sub(url_pattern, '', sentence)
    sentence = sentence.replace("\u200b", "")
    # results2 = re.compile(r'[//@].*?[:]', re.S)
    # sentence = re.sub(results2, '', sentence)
    # sentence = sentence.replace("\n", " ")
    sentence = sentence.strip()
    if len(sentence) < 10:
        return None
    return sentence