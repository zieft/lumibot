import json
import os
import random
import re
import time
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright


class PropertyScraper(ABC):
    """
    房源爬虫的抽象基类，定义了爬虫的基本接口
    """

    def __init__(self):
        # 通用请求头
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
        }

    @abstractmethod
    def get_property_links(self, base_url: str, params: Dict[str, Any], pages: int) -> List[str]:
        """
        获取房源详情页链接

        参数:
            base_url: 基础URL
            params: URL参数
            pages: 爬取的页数

        返回:
            List[str]: 房源详情页链接列表
        """
        pass

    @abstractmethod
    def get_property_details(self, url: str) -> Optional[str]:
        """
        获取房源详情页内容

        参数:
            url: 房源详情页URL

        返回:
            Optional[str]: 处理后的房源详情，获取失败则返回None
        """
        pass

    @abstractmethod
    def extract_property_id(self, url: str) -> str:
        """
        从URL中提取房源ID

        参数:
            url: 房源详情页URL

        返回:
            str: 房源ID
        """
        pass


class KleinanzeigenScraper(PropertyScraper):
    """
    Kleinanzeigen.de网站的爬虫实现
    """

    def get_property_links(self, base_url: str, params: Dict[str, Any], pages: int) -> List[str]:
        """
        从Kleinanzeigen获取房源详情页链接

        参数:
            base_url: 基础URL (如 "https://www.kleinanzeigen.de/s-wohnung-mieten/aachen/")
            params: URL参数 (如 {"code": "k0c203l1921"})
            pages: 爬取的页数

        返回:
            List[str]: 房源详情页链接列表
        """
        links = []
        url2 = params.get("code", "")

        for i in range(1, pages + 1):
            # 爬取i页信息
            if i == 1:
                paged_url = base_url + url2
            else:
                paged_url = base_url + f"seite:{i}/" + url2

            response = requests.get(paged_url, headers=self.headers)

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

                print(f"第{i}页获取到的链接数量：{len(links)}")
            else:
                print(f"获取页面失败，状态码: {response.status_code}")

            # 添加短暂延迟
            if i < pages:
                time.sleep(1)

        return links

    def get_property_details(self, url: str) -> Optional[str]:
        """
        获取Kleinanzeigen房源详情页内容

        参数:
            url: 房源详情页URL

        返回:
            Optional[str]: 处理后的房源详情，获取失败则返回None
        """
        response = requests.get(url, headers=self.headers)

        # 确保正确设置编码
        response.encoding = "utf-8"

        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')

            # 将 HTML 转换为纯文本并去除多余空白符
            article_text = soup.get_text(separator=" ").strip()
            decoded_text = article_text.encode().decode('unicode_escape')
            decoded_text = " ".join(decoded_text.split())  # 去除多余的空格符

            # 清理内容
            cleaned_text = self._remove_unwanted_content(decoded_text)

            # 添加原始URL到文本中，便于后续跟踪
            return cleaned_text + f' Website: {url}'
        else:
            print(f"无法获取页面 {url}，状态码: {response.status_code}")
            return None

    def _remove_unwanted_content(self, text: str) -> str:
        """
        移除Kleinanzeigen网页中不需要的内容

        参数:
            text: 原始网页文本

        返回:
            str: 清理后的文本
        """
        # 定义开始和结束标记
        start_marker = r"via E-Mail teilen via Facebook teilen via X teilen via Pinterest teilen"
        end_marker = r"Anzeige melden Anzeige drucken"

        # 匹配并删除开始标记之前的内容
        text = re.sub(rf"^.*?{re.escape(start_marker)}", start_marker, text, flags=re.DOTALL)

        # 匹配并删除结束标记之后的内容
        text = re.sub(rf"{re.escape(end_marker)}.*$", end_marker, text, flags=re.DOTALL)

        return text

    def extract_property_id(self, url: str) -> str:
        """
        从Kleinanzeigen URL中提取房源ID

        参数:
            url: 房源详情页URL

        返回:
            str: 房源ID
        """
        # 提取 "s-anzeige/" 后面的部分作为ID
        return url.split('/s-anzeige/')[-1]


class ImmobilienScout24Scraper(PropertyScraper):
    """
    ImmobilienScout24.de网站的爬虫实现
    """

    def get_property_links(self, base_url: str, params: Dict[str, Any], pages: int) -> List[str]:
        """
        从ImmobilienScout24获取房源详情页链接

        参数:
            base_url: 基础URL
            params: URL参数
            pages: 爬取的页数

        返回:
            List[str]: 房源详情页链接列表
        """
        links = []

        for page in range(1, pages + 1):
            # 构建分页URL
            page_url = f"{base_url}?pagenumber={page}"

            # 添加其他参数
            for key, value in params.items():
                if key != "pagenumber":
                    page_url += f"&{key}={value}"

            try:
                response = requests.get(page_url, headers=self.headers)

                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')

                    # 找出所有房源卡片
                    property_cards = soup.select('article.result-list-entry')

                    for card in property_cards:
                        # 在卡片中找到链接
                        link_elem = card.select_one('a.result-list-entry__brand-title-container')
                        if link_elem and 'href' in link_elem.attrs:
                            # 提取href属性
                            href = link_elem['href']
                            # 处理相对URL
                            if href.startswith('/'):
                                full_link = f"https://www.immobilienscout24.de{href}"
                            else:
                                full_link = href
                            links.append(full_link)

                    print(f"第{page}页获取到的链接数量：{len(links)}")
                else:
                    print(f"获取页面失败，状态码: {response.status_code}")

            except Exception as e:
                print(f"爬取第{page}页时出错: {e}")

            # 添加延迟，避免请求过于频繁
            if page < pages:
                time.sleep(1.5)

        return links

    def get_property_details(self, url: str) -> Optional[str]:
        """
        获取ImmobilienScout24房源详情页内容

        参数:
            url: 房源详情页URL

        返回:
            Optional[str]: 处理后的房源详情，获取失败则返回None
        """
        try:
            response = requests.get(url, headers=self.headers)
            response.encoding = "utf-8"

            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')

                # 移除不需要的元素
                for element in soup.select('header, footer, nav, .is24-scoutad-container'):
                    element.decompose()

                # 获取主要内容
                main_content = soup.select_one('div.grid-item.padding-desk-horizontal-l')

                if main_content:
                    # 提取文本并清理
                    content_text = main_content.get_text(separator=" ").strip()
                    content_text = " ".join(content_text.split())

                    # 添加原始URL
                    return content_text + f' Website: {url}'
                else:
                    # 如果找不到主要内容，使用整个页面
                    content_text = soup.get_text(separator=" ").strip()
                    content_text = " ".join(content_text.split())
                    return content_text + f' Website: {url}'
            else:
                print(f"获取页面失败，状态码: {response.status_code}")
                return None

        except Exception as e:
            print(f"获取房源详情时出错: {e}")
            return None

    def extract_property_id(self, url: str) -> str:
        """
        从ImmobilienScout24 URL中提取房源ID

        参数:
            url: 房源详情页URL

        返回:
            str: 房源ID
        """
        # 示例URL: https://www.immobilienscout24.de/expose/123456789
        match = re.search(r'/expose/(\d+)', url)
        if match:
            return match.group(1)
        else:
            # 如果无法提取ID，使用URL的哈希值
            return str(hash(url))


class PropertyCrawler:
    """
    房源爬虫主类，支持多个房源网站
    """

    def __init__(self, output_dir: str = "data"):
        """
        初始化爬虫

        参数:
            output_dir: 输出数据目录
        """
        self.output_dir = output_dir

        # 确保输出目录存在
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # 确保sites目录存在
        if not os.path.exists("sites"):
            os.makedirs("sites")

        # 注册支持的爬虫
        self.scrapers = {
            "kleinanzeigen.de": KleinanzeigenScraper(),
            "immobilienscout24.de": ImmobilienScout24Scraper(),
        }

    def get_scraper_for_url(self, url: str) -> Optional[PropertyScraper]:
        """
        根据URL获取对应的爬虫

        参数:
            url: 网站URL

        返回:
            Optional[PropertyScraper]: 对应的爬虫，如果不支持则返回None
        """
        domain = urlparse(url).netloc

        # 遍历注册的爬虫，查找匹配的域名
        for domain_key, scraper in self.scrapers.items():
            if domain_key in domain:
                return scraper

        print(f"不支持的网站域名: {domain}")
        return None

    def register_scraper(self, domain: str, scraper: PropertyScraper):
        """
        注册新的爬虫

        参数:
            domain: 域名
            scraper: 爬虫实例
        """
        self.scrapers[domain] = scraper
        print(f"成功注册爬虫: {domain}")

    def crawl_properties(self, base_url: str, params: Dict[str, Any], pages: int = 1) -> Dict[str, str]:
        """
        爬取房源数据

        参数:
            base_url: 基础URL
            params: URL参数
            pages: 爬取的页数

        返回:
            Dict[str, str]: 房源数据字典，键为房源ID，值为网页内容
        """
        # 获取适用的爬虫
        scraper = self.get_scraper_for_url(base_url)
        if not scraper:
            return {}

        # 获取房源链接
        print(f"开始获取房源链接...")
        links = scraper.get_property_links(base_url, params, pages)
        print(f"共获取到 {len(links)} 个房源链接")

        # 存储网页内容的字典
        webpage_dict = {}

        # 遍历每个链接，获取房源详情
        print(f"开始获取房源详情...")
        for i, link in enumerate(links):
            print(f"正在处理 {i + 1}/{len(links)}: {link}")

            # 获取房源详情
            property_details = scraper.get_property_details(link)

            if property_details:
                # 提取房源ID
                property_id = scraper.extract_property_id(link)

                # 存储结果
                webpage_dict[property_id] = property_details
                print(f"已保存房源 {property_id}")

            # 添加短暂延迟，避免请求过于频繁
            if i < len(links) - 1:
                time.sleep(0.5)

        print(f"完成房源爬取，共获取 {len(webpage_dict)} 个有效房源")
        return webpage_dict

    def save_to_json(self, data: Dict[str, Any], filename: str) -> None:
        """
        将数据保存为JSON文件

        参数:
            data: 要保存的数据
            filename: 文件名（不含路径）
        """
        filepath = os.path.join(self.output_dir, filename)
        with open(filepath, 'w', encoding='utf-8') as json_file:
            json.dump(data, json_file, indent=4, ensure_ascii=False)
        print(f"数据已保存至: {filepath}")


def get_property_links_playwright(base_url, pages=1):
    links = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)  # 设置为True可隐藏浏览器窗口
        context = browser.new_context()
        page = context.new_page()

        # 访问网站
        page.goto(base_url)
        time.sleep(3)  # 等待加载

        for i in range(1, pages + 1):
            if i > 1:
                # 导航到下一页
                next_button = page.locator('a[data-nav-next="true"]')
                if next_button.count() > 0:
                    next_button.click()
                    page.wait_for_load_state('networkidle')
                else:
                    break

            # 提取所有房源链接
            property_links = page.eval_on_selector_all(
                'article.result-list-entry a.result-list-entry__brand-title-container',
                'elements => elements.map(el => el.href)')

            links.extend(property_links)
            print(f"第{i}页获取到的链接数量：{len(property_links)}")

            time.sleep(random.uniform(2, 4))

        browser.close()

    return links


# 使用示例
if __name__ == "__main__":
    # 创建爬虫实例
    crawler = PropertyCrawler()

    # 根据命令行参数选择爬取的网站
    import argparse

    parser = argparse.ArgumentParser(description='房源爬虫')
    parser.add_argument('--website', type=str, choices=['kleinanzeigen', 'immoscout24'], default='kleinanzeigen',
                        help='要爬取的网站 (kleinanzeigen 或 immoscout24)')
    parser.add_argument('--pages', type=int, default=1, help='爬取的页数')
    parser.add_argument('--output', type=str, default='property_data.json', help='输出文件名')
    args = parser.parse_args()

    # 根据选择的网站设置参数
    if args.website == 'kleinanzeigen':
        base_url = "https://www.kleinanzeigen.de/s-wohnung-mieten/aachen/"
        params = {"code": "k0c203l1921"}  # k0：表示关键字为空，c203：租房类别，l1921：亚琛
    else:  # immoscout24
        base_url = "https://www.immobilienscout24.de/Suche/de/nordrhein-westfalen/aachen/wohnung-mieten?enteredFrom=one_step_search"
        params = {}  # 示例：价格不超过900欧，面积40平以上

    # 爬取房源数据
    webpage_dict = crawler.crawl_properties(base_url, params, args.pages)

    # 保存爬取结果
    crawler.save_to_json(webpage_dict, args.output)
