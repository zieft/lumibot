import requests
from bs4 import BeautifulSoup
import os
import time
import tiktoken
import json
import re

from pydantic import BaseModel, Field
from typing import List, Optional
from gpt_client import gpt_client

url_1 = "https://www.kleinanzeigen.de/s-wohnung-mieten/aachen/"
url_2 = "k0c203l1921"  # todo: 这个代码可以解耦, k0：表示关键字为空，即未输入特定的搜索关键词。c203：租房类别。l1921：亚琛。


# TODO: kleinanzeigen完成后再适配其他网站

def get_sub_links(url1, url2):
    """ Kleinanzeigen的域名解析 """
    # 存储所有链接
    links = []

    for i in range(1, 2):
        # 爬取i页信息
        if i == 1:
            paged_url = url1 + url2
        else:
            paged_url = url1 + f"seite:{i}/" + url2

        response = requests.get(paged_url)

        if response.status_code == 200:
            # 解析 HTML 内容
            soup = BeautifulSoup(response.text, 'html.parser')

            # 查找所有 <li> 标签
            li_tags = soup.find_all('li')

            # 遍历每个 <li> 标签
            for li in li_tags:
                # 查找 <li> 中的 <a> 标签
                a_tag = li.find('a', href=True)
                if a_tag and a_tag['href'].startswith('/s-anzeige'):
                    full_link = "https://www.kleinanzeigen.de" + a_tag['href']
                    links.append(full_link)

            # 打印所有找到的链接
            print("获取到的链接：")
            for link in links:
                print(link)
        else:
            print(f"获取页面失败，状态码: {response.status_code}")

    return links


# 确保有一个 "sites" 文件夹存在
output_dir = "sites"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)


def remove_unwanted_content(text):
    # 定义开始和结束标记
    start_marker = r"via E-Mail teilen via Facebook teilen via X teilen via Pinterest teilen"
    end_marker = r"Anzeige melden Anzeige drucken"

    # 匹配并删除开始标记之前的内容
    text = re.sub(rf"^.*?{re.escape(start_marker)}", start_marker, text, flags=re.DOTALL)

    # 匹配并删除结束标记之后的内容
    text = re.sub(rf"{re.escape(end_marker)}.*$", end_marker, text, flags=re.DOTALL)

    # 删除start & end marker
    # text = text.replace(start_marker, "")
    # text = text.replace(end_marker, "")

    return text


def get_text(url, website='kleinanzeigen'):
    # 自定义请求头
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",  # 指定接受的语言
        "Accept-Encoding": "gzip, deflate, br",  # 启用压缩
        "Connection": "keep-alive",  # 保持长连接
    }

    # 发送带有自定义头的请求
    response = requests.get(url, headers=headers)

    # 确保正确设置编码
    response.encoding = "utf-8"  # 强制设置响应的编码为 UTF-8

    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')

        # 将 HTML 转换为纯文本并去除多余空白符
        article_text = soup.get_text(separator=" ").strip()
        decoded_text = article_text.encode().decode('unicode_escape')
        decoded_text = " ".join(decoded_text.split())  # 去除多余的空格符
        decoded_text = remove_unwanted_content(decoded_text)  # 去除多余的短句
        # fixed_encoding = fix_encoding(decoded_text)
        return decoded_text
    else:
        print(f"无法获取页面 {url}，状态码: {response.status_code}")
        return None


# 存储网页内容的字典
webpage_dict = {}

links = get_sub_links(url_1, url_2)

# 遍历每个链接，获取并存储清理后的 HTML 内容
for link in links:
    pre_cleaned_text = get_text(link)
    if pre_cleaned_text:
        # 提取 "s-anzeige/" 后面的部分作为字典的键
        key = link.split('/s-anzeige/')[-1]
        webpage_dict[key] = pre_cleaned_text + 'Website: https://www.kleinanzeigen.de/s-anzeige/' + key
        print(f"已保存内容到字典，键: {key}")

# 打印字典内容以验证
# print(webpage_dict)

# 将字典保存为 JSON 文件
with open('data/data.json', 'w') as json_file:
    json.dump(webpage_dict, json_file, indent=4)  # indent参数使JSON文件更易读


## Structural output method
class PropertyInfo(BaseModel):
    title: str
    full_website: str
    landlord: Optional[str]
    WG: Optional[bool]
    location: str
    area_sqm: int
    rooms: int
    bedrooms: int
    bathrooms: int
    floor: int
    rent: int
    neben_kosten: int
    deposit: int
    available_from: str
    rental_period_end: Optional[str] = None
    features: List[str] = Field(default_factory=list)
    requirements: list[str]
    untermieten: bool = Field(default_factory=lambda: False)
    zwischenmieten: bool = Field(default_factory=lambda: False)
    tauschwohnung: bool = Field(default_factory=lambda: False)
    classification: str

    class Config:
        allow_population_by_field_name = True  # 支持使用字段别名传入数据


property_analyzer_system = """
提取信息,
WG:是否可以组建wg;
landlord:请选择:['company', 'privat'];
requirements:请在下列列表中选择:['student','Wohnberechtigungsschein'];
classification:给该房源评级:['luxury','decent','fair'];
untermieten:具有明确结束日期的为True;
tauschwohnung:通常这样的房主会找另一个房主交换房屋;
"""


def analyze_property_info_structural_output(web_content, gpt_client, system_content):
    completion = gpt_client.beta.chat.completions.parse(
        model="gpt-4o-2024-08-06",
        messages=[
            {"role": "system", "content": system_content},
            {"role": "user", "content": web_content},
        ],
        response_format=PropertyInfo
    )

    event = completion.choices[0].message.parsed
    event_dict = event.model_dump()
    # 去掉'''python'''字样
    # event_dict = re.sub(r"```python|```|'''python|'''", "", event_dict).strip()
    event_json = json.dumps(event_dict, indent=4, ensure_ascii=False)

    return event_dict


def count_tokens(text, model="gpt-4o"):
    # 根据模型加载适配的编码器
    encoding = tiktoken.encoding_for_model(model)
    # 将字符串编码为 tokens
    tokens = encoding.encode(text)
    # 返回 token 数量
    return len(tokens)


# 存储分析结果的字典
analysis_results = {}

# 遍历网页数据字典，发送每个内容到 GPT API 进行分析
property_count = 1
tokens_total = 0
for key, content in webpage_dict.items():
    token_len = count_tokens(content)
    tokens_total += token_len
    analyzed_result = analyze_property_info_structural_output(content, gpt_client, property_analyzer_system)
    # 将原html存入数据库
    analyzed_result['raw_html'] = content
    analysis_results[key] = analyzed_result
    property_count += 1
    print(f"已分析房源: {key}, token数量: {token_len}")
print(f"共分析房源{property_count}个, 输入token总量为: {tokens_total}")

# 将分析结果保存为 JSON 文件
output_file = "data/property_analysis_results3.json"
with open(output_file, 'w', encoding='utf-8') as f:
    json.dump(analysis_results, f, ensure_ascii=False, indent=4)

print(f"分析结果已保存为: {output_file}")
