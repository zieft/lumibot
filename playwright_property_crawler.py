import json
import os
import random
import re
import time
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from urllib.parse import urlparse

from playwright.sync_api import sync_playwright, Page


class PlaywrightPropertyScraper(ABC):
    """
    使用Playwright的房源爬虫抽象基类
    """

    def __init__(self):
        """初始化爬虫基类"""
        pass

    @abstractmethod
    def get_property_links(self, page: Page, base_url: str, params: Dict[str, Any], pages: int) -> List[str]:
        """
        获取房源详情页链接

        参数:
            page: Playwright页面对象
            base_url: 基础URL
            params: URL参数
            pages: 爬取的页数

        返回:
            List[str]: 房源详情页链接列表
        """
        pass

    @abstractmethod
    def get_property_details(self, page: Page, url: str) -> Optional[str]:
        """
        获取房源详情页内容

        参数:
            page: Playwright页面对象
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

    # 新增: 模拟人类行为的辅助方法
    def perform_human_like_behavior(self, page: Page):
        """
        执行一系列模拟人类的行为，以避免被检测为机器人
        """
        # 随机鼠标移动
        self._perform_random_mouse_movements(page)

        # 随机滚动页面
        self._scroll_randomly(page)

        # 模拟阅读行为（停留在页面上一段时间）
        time.sleep(random.uniform(3, 8))

    def _perform_random_mouse_movements(self, page: Page):
        """
        模拟人类随机鼠标移动行为
        """
        # 执行3-8次随机鼠标移动
        for _ in range(random.randint(3, 8)):
            # 移动到随机位置，使用steps参数让移动更自然
            page.mouse.move(
                random.randint(100, 800),
                random.randint(100, 600),
                steps=random.randint(5, 15)  # 移动鼠标的步骤数（越多越平滑）
            )
            # 每次移动后短暂停顿
            page.wait_for_timeout(random.uniform(200, 1000))

        # 偶尔执行点击
        if random.random() < 0.3:  # 30%的概率
            # 点击页面上安全的区域（避免链接或按钮）
            page.mouse.click(random.randint(300, 500), random.randint(200, 300))

    def _scroll_randomly(self, page: Page):
        """
        模拟人类随机滚动行为
        """
        # 可能的滚动距离
        scroll_distances = [100, 200, 300, 150, 250, -100, -150]

        # 执行2-5次随机滚动
        for _ in range(random.randint(2, 5)):
            # 选择随机滚动距离
            distance = random.choice(scroll_distances)

            # 执行滚动
            page.evaluate(f"window.scrollBy(0, {distance})")

            # 滚动后等待随机时间，模拟阅读内容
            page.wait_for_timeout(random.uniform(500, 2500))


class KleinanzeigenPlaywrightScraper(PlaywrightPropertyScraper):
    """Kleinanzeigen.de网站的Playwright爬虫实现"""

    def get_property_links(self, page: Page, base_url: str, params: Dict[str, Any], pages: int) -> List[str]:
        """
        从Kleinanzeigen获取房源详情页链接

        参数:
            page: Playwright页面对象
            base_url: 基础URL (如 "https://www.kleinanzeigen.de/s-wohnung-mieten/aachen/")
            params: URL参数 (如 {"code": "k0c203l1921"})
            pages: 爬取的页数

        返回:
            List[str]: 房源详情页链接列表
        """
        links = []
        url2 = params.get("code", "")

        for i in range(1, pages + 1):
            # 构建URL
            if i == 1:
                paged_url = base_url + url2
            else:
                paged_url = base_url + f"seite:{i}/" + url2

            print(f"正在访问页面: {paged_url}")

            try:
                # 导航到页面，增加timeout防止加载错误
                page.goto(paged_url, wait_until="domcontentloaded", timeout=60000)

                # 修改：随机等待一段时间，模拟人类浏览行为
                page.wait_for_timeout(random.uniform(2000, 5000))

                # 修改：执行人类行为模拟
                self.perform_human_like_behavior(page)

                # 等待页面加载完成
                page.wait_for_load_state("networkidle", timeout=30000)

                # 等待房源列表加载
                page.wait_for_selector("ul.ad-list", timeout=30000)

                # 获取所有房源链接
                link_elements = page.query_selector_all("li a[href^='/s-anzeige']")

                page_links = []
                for element in link_elements:
                    href = element.get_attribute("href")
                    if href:
                        full_link = "https://www.kleinanzeigen.de" + href
                        page_links.append(full_link)

                # 去重
                page_links = list(set(page_links))
                links.extend(page_links)

                print(f"第{i}页获取到的链接数量: {len(page_links)}")

                # 修改：使用更自然的随机延迟
                time.sleep(random.uniform(3, 7))

            except Exception as e:
                print(f"处理第{i}页时发生错误: {e}")
                # 保存当前页面截图和HTML以便调试
                page.screenshot(path=f"error_page_{i}.png")
                with open(f"error_page_{i}.html", "w", encoding="utf-8") as f:
                    f.write(page.content())

        return links

    def get_property_details(self, page: Page, url: str) -> Optional[str]:
        """
        获取Kleinanzeigen房源详情页内容

        参数:
            page: Playwright页面对象
            url: 房源详情页URL

        返回:
            Optional[str]: 处理后的房源详情，获取失败则返回None
        """
        try:
            print(f"正在获取房源详情: {url}")

            # 导航到详情页，增加超时时间
            page.goto(url, wait_until="domcontentloaded", timeout=60000)

            # 修改：执行人类行为模拟
            self.perform_human_like_behavior(page)

            # 等待页面加载完成，延长等待时间
            page.wait_for_load_state("networkidle", timeout=60000)

            # 随机滚动页面，模拟人类行为
            page.evaluate("window.scrollTo(0, document.body.scrollHeight * 0.3)")
            time.sleep(random.uniform(1, 2))
            page.evaluate("window.scrollTo(0, document.body.scrollHeight * 0.7)")
            time.sleep(random.uniform(1, 2))

            # 保存当前页面截图以便分析
            page_id = self.extract_property_id(url)
            page.screenshot(path=f"page_{page_id}.png")

            # 直接从浏览器中提取文本内容，不依赖特定的选择器
            text = page.evaluate("""() => {
                // 移除不需要的元素的文本
                const elementsToRemove = document.querySelectorAll('header, footer, nav, script, style');
                for (const el of elementsToRemove) {
                    if (el && el.textContent) el.textContent = '';
                }

                // 获取主要内容，尝试多种可能的选择器
                let mainContent = document.querySelector('#viewad-main-container');
                if (!mainContent) mainContent = document.querySelector('.addetailspage');
                if (!mainContent) mainContent = document.querySelector('article');
                if (!mainContent) mainContent = document.querySelector('.l-container');
                if (!mainContent) mainContent = document.body; // 如果都找不到，使用整个body

                return mainContent.innerText || document.body.innerText;
            }""")

            if text:
                # 清理文本
                text = re.sub(r'\s+', ' ', text).strip()

                # 使用标记进行过滤
                cleaned_text = self._remove_unwanted_content(text)

                # 添加原始URL
                return cleaned_text + f' Website: {url}'
            else:
                # 如果JavaScript提取失败，尝试使用BeautifulSoup作为备选方案
                print(f"JavaScript提取失败，尝试使用BeautifulSoup: {url}")

                # 获取页面内容
                content = page.content()

                # 使用BeautifulSoup解析内容
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(content, 'html.parser')

                # 移除不需要的元素
                for element in soup.select('header, footer, nav, script, style'):
                    if element:
                        element.decompose()

                # 提取全部文本，不依赖特定容器
                text = soup.get_text(separator=" ").strip()
                text = re.sub(r'\s+', ' ', text)  # 删除多余空白

                # 使用标记进行过滤
                cleaned_text = self._remove_unwanted_content(text)

                # 添加原始URL
                return cleaned_text + f' Website: {url}'

        except Exception as e:
            print(f"获取房源详情时出错: {e}")
            # 保存错误页面以便调试
            page.screenshot(path=f"error_detail_{self.extract_property_id(url)}.png")
            # 保存页面HTML
            try:
                with open(f"error_detail_{self.extract_property_id(url)}.html", "w", encoding="utf-8") as f:
                    f.write(page.content())
            except:
                pass
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

        # 尝试匹配并删除开始标记之前的内容
        start_match = re.search(re.escape(start_marker), text)
        if start_match:
            text = text[start_match.start():]

        # 尝试匹配并删除结束标记之后的内容
        end_match = re.search(re.escape(end_marker), text)
        if end_match:
            text = text[:end_match.end()]

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


class ImmobilienScout24PlaywrightScraper(PlaywrightPropertyScraper):
    """ImmobilienScout24.de网站的Playwright爬虫实现"""

    def get_property_links(self, page: Page, base_url: str, params: Dict[str, Any], pages: int) -> List[str]:
        """
        从ImmobilienScout24获取房源详情页链接

        参数:
            page: Playwright页面对象
            base_url: 基础URL
            params: URL参数
            pages: 爬取的页数

        返回:
            List[str]: 房源详情页链接列表
        """
        links = []

        # 构建初始URL（添加参数）
        url = base_url
        first = True
        for key, value in params.items():
            separator = "?" if first else "&"
            url += f"{separator}{key}={value}"
            first = False

        try:
            # 修改：随机初始等待，模拟人类打开页面行为
            page.wait_for_timeout(random.uniform(2000, 5000))

            # 访问首页
            print(f"正在访问页面: {url}")

            # 修改：使用更长的超时时间，并使用load事件而不是domcontentloaded
            page.goto(url, wait_until="load", timeout=90000)

            # 修改：执行人类行为模拟
            self.perform_human_like_behavior(page)

            # 修改：增强Cookie处理逻辑
            try:
                # 尝试多种可能的Cookie接受按钮选择器
                cookie_selectors = [
                    "button#consent-banner-btn-accept-all",
                    "button[data-testid='uc-accept-all-button']",
                    "button.consent-accept-all",
                    "button.consent-btn-accept-all",
                    "button[data-gdpr-accept-all]",
                    ".cookie-alert-extended-button-secondary",
                    "button.message-component.message-button.no-children.focusable.sp_choice_type_11"
                ]

                for selector in cookie_selectors:
                    try:
                        # 尝试定位Cookie按钮
                        consent_button = page.query_selector(selector)
                        if consent_button:
                            # 如果找到按钮，点击它
                            consent_button.click()
                            print(f"成功点击Cookie接受按钮: {selector}")
                            page.wait_for_timeout(3000)  # 等待Cookie弹窗消失
                            break
                    except Exception as cookie_error:
                        print(f"尝试选择器 {selector} 时出错: {cookie_error}")
                        continue
            except Exception as e:
                print(f"处理Cookie提示时出错: {e}")

            for i in range(1, pages + 1):
                # 在首页之后，使用分页功能
                if i > 1:
                    print(f"正在导航到第{i}页")

                    # 检查分页元素是否存在
                    pager = page.query_selector("nav.react-carousel__list")
                    if not pager:
                        print(f"未找到分页元素，可能没有更多页")
                        break

                    # 修改：滚动到分页区域，使其可见
                    try:
                        page.evaluate("""() => {
                            const pager = document.querySelector('nav.react-carousel__list');
                            if (pager) {
                                pager.scrollIntoView({ behavior: 'smooth', block: 'center' });
                            }
                        }""")
                        page.wait_for_timeout(1000)  # 等待滚动完成
                    except Exception as e:
                        print(f"滚动到分页区域时出错: {e}")

                    # 尝试点击下一页按钮
                    try:
                        # 修改：使用更多可能的选择器
                        next_button_selectors = [
                            "button[data-nav-next='true']",
                            "button.pagination-next",
                            "a.pagination__nav-item--next",
                            "button[data-testid='next-page-button']"
                        ]

                        next_button = None
                        for selector in next_button_selectors:
                            next_button = page.query_selector(selector)
                            if next_button and next_button.is_enabled():
                                break

                        if next_button and next_button.is_enabled():
                            # 修改：增加点击前的等待和随机动作
                            page.wait_for_timeout(random.uniform(500, 1500))

                            # 移动鼠标到按钮上方
                            next_button_box = next_button.bounding_box()
                            if next_button_box:
                                page.mouse.move(
                                    next_button_box["x"] + next_button_box["width"] / 2,
                                    next_button_box["y"] + next_button_box["height"] / 2
                                )
                                page.wait_for_timeout(random.uniform(300, 800))

                            # 点击下一页
                            next_button.click()

                            # 修改：使用更长的等待时间，确保页面加载完成
                            page.wait_for_load_state("networkidle", timeout=45000)
                            print(f"已导航到第{i}页")

                            # 修改：执行人类行为模拟
                            self.perform_human_like_behavior(page)
                        else:
                            print(f"无法找到或点击下一页按钮，可能已到最后一页")
                            break
                    except Exception as e:
                        print(f"点击下一页按钮时出错: {e}")
                        page.screenshot(path=f"pagination_error_page_{i}.png")
                        break

                # 修改：使用更多可能的列表选择器
                selectors = [
                    "article.result-list-entry",
                    "div.result-list__listing",
                    "ul.result-list__listing li",
                    "div[data-testid='result-list-entry']"
                ]

                # 尝试所有可能的选择器
                property_cards = []
                for selector in selectors:
                    try:
                        cards = page.query_selector_all(selector)
                        if cards and len(cards) > 0:
                            property_cards = cards
                            print(f"使用选择器找到房源卡片: {selector}")
                            break
                    except Exception:
                        continue

                if not property_cards:
                    print("未能找到房源列表，尝试使用备用方法...")

                    # 保存页面以便调试
                    page.screenshot(path=f"no_cards_page_{i}.png")

                    # 修改：备用提取方法 - 使用评估JavaScript获取链接
                    try:
                        page_links = page.evaluate("""() => {
                            // 尝试查找包含"/expose/"的链接
                            const allLinks = Array.from(document.querySelectorAll('a[href*="/expose/"]'));
                            return allLinks.map(link => link.href);
                        }""")

                        if page_links and len(page_links) > 0:
                            links.extend(page_links)
                            print(f"使用备用JavaScript方法获取了 {len(page_links)} 个链接")
                        else:
                            print("备用JavaScript方法未找到链接")
                    except Exception as js_error:
                        print(f"执行备用JavaScript提取时出错: {js_error}")
                else:
                    page_links = []
                    for card in property_cards:
                        try:
                            # 修改：使用更多可能的链接选择器
                            link_selectors = [
                                "a.result-list-entry__brand-title-container",
                                "a[data-testid='result-list-entry-link']",
                                "a.result-list-entry__link"
                            ]

                            link_element = None
                            for selector in link_selectors:
                                link_element = card.query_selector(selector)
                                if link_element:
                                    break

                            if link_element:
                                href = link_element.get_attribute("href")
                                if href:
                                    # 处理相对URL
                                    if href.startswith('/'):
                                        full_link = f"https://www.immobilienscout24.de{href}"
                                    else:
                                        full_link = href
                                    page_links.append(full_link)
                        except Exception as card_error:
                            print(f"处理房源卡片时出错: {card_error}")
                            continue

                    links.extend(page_links)
                    print(f"第{i}页获取到的链接数量: {len(page_links)}")

                # 修改：更自然的随机延迟，避免被检测
                delay = random.uniform(5, 10)  # 使用更长的延迟
                print(f"等待 {delay:.1f} 秒...")
                time.sleep(delay)

        except Exception as e:
            print(f"获取房源链接时发生错误: {e}")
            # 保存当前页面截图和HTML以便调试
            page.screenshot(path="error_list_page.png")
            with open("error_list_page.html", "w", encoding="utf-8") as f:
                f.write(page.content())

        return links

    def get_property_details(self, page: Page, url: str) -> Optional[str]:
        """
        获取ImmobilienScout24房源详情页内容

        参数:
            page: Playwright页面对象
            url: 房源详情页URL

        返回:
            Optional[str]: 处理后的房源详情，获取失败则返回None
        """
        try:
            print(f"正在获取房源详情: {url}")

            # 修改：随机初始等待
            page.wait_for_timeout(random.uniform(2000, 5000))

            # 修改：使用更长的超时时间和加载事件
            page.goto(url, wait_until="load", timeout=90000)

            # 等待页面加载完成
            page.wait_for_load_state("networkidle", timeout=45000)

            # 修改：执行人类行为模拟
            self.perform_human_like_behavior(page)

            # 修改：增强Cookie处理逻辑
            try:
                cookie_selectors = [
                    "button#consent-banner-btn-accept-all",
                    "button[data-testid='uc-accept-all-button']",
                    "button.consent-accept-all"
                ]

                for selector in cookie_selectors:
                    try:
                        consent_button = page.query_selector(selector)
                        if consent_button:
                            consent_button.click()
                            print(f"成功点击Cookie接受按钮: {selector}")
                            page.wait_for_timeout(3000)
                            break
                    except Exception:
                        continue
            except Exception:
                pass  # 忽略Cookie处理错误

            # 修改：使用多个可能的选择器，并增加等待时间
            selectors = [
                ".is24-scoutad__splitter",
                "div[data-testid='is24-expose-section']",
                "div.is24-content",
                "div#is24-content",
                "main.main-container"
            ]

            found_selector = None
            for selector in selectors:
                try:
                    if page.query_selector(selector):
                        found_selector = selector
                        print(f"找到内容选择器: {selector}")
                        break
                except Exception:
                    continue

            if found_selector:
                page.wait_for_selector(found_selector, timeout=45000)
            else:
                print("未找到任何内容选择器，将尝试获取全页内容")

            # 修改：执行更多滚动行为，确保所有内容都加载
            total_height = page.evaluate("document.body.scrollHeight")
            viewport_height = page.evaluate("window.innerHeight")

            if total_height > viewport_height:
                # 分段滚动，模拟阅读
                steps = min(int(total_height / viewport_height) + 1, 10)  # 最多10次滚动
                for i in range(1, steps + 1):
                    # 滚动到页面指定百分比
                    scroll_position = (i / steps) * total_height
                    page.evaluate(f"window.scrollTo(0, {scroll_position})")

                    # 模拟阅读行为，随机停留
                    page.wait_for_timeout(random.uniform(1000, 3000))

            # 获取页面内容
            content = page.content()

            # 使用BeautifulSoup解析内容
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(content, 'html.parser')

            # 移除不需要的元素
            for element in soup.select('header, footer, nav, script, style, .is24-scoutad-container'):
                if element:
                    element.decompose()

            # 获取主要内容区域
            main_content = None
            content_selectors = [
                ".is24-scoutad__splitter",
                "div[data-testid='is24-expose-section']",
                "div.is24-content",
                "div#is24-content",
                "main.main-container"
            ]

            for selector in content_selectors:
                main_content = soup.select_one(selector)
                if main_content:
                    break

            if main_content:
                # 提取并清理文本
                text = main_content.get_text(separator=" ").strip()
                text = re.sub(r'\s+', ' ', text)  # 删除多余空白

                # 添加原始URL
                return text + f' Website: {url}'
            else:
                # 修改：增强备用提取方法
                print("未找到主要内容区域，尝试提取结构化信息...")

                # 尝试使用JavaScript提取关键信息
                property_info = page.evaluate("""() => {
                    const info = {};

                    // 尝试提取标题
                    const title = document.querySelector('h1');
                    if (title) info.title = title.textContent.trim();

                    // 尝试提取价格
                    const priceEl = document.querySelector('[data-testid="is24-expose-data-price"]') || 
                                   document.querySelector('.is24-value.is24-value-font-strong');
                    if (priceEl) info.price = priceEl.textContent.trim();

                    // 尝试提取地址
                    const addressEl = document.querySelector('[data-testid="is24-expose-address"]') ||
                                     document.querySelector('.address-with-map-link');
                    if (addressEl) info.address = addressEl.textContent.trim();

                    // 提取所有详情字段
                    const detailRows = document.querySelectorAll('.grid-item') || 
                                      document.querySelectorAll('.criteriagroup .criteria');
                    if (detailRows.length > 0) {
                        info.details = Array.from(detailRows).map(row => row.textContent.trim()).join(' | ');
                    }

                    // 提取描述
                    const descEl = document.querySelector('#contentDescription') ||
                                  document.querySelector('.is24-long-text');
                    if (descEl) info.description = descEl.textContent.trim();

                    return info;
                }""")

                if property_info and (
                        property_info.get('title') or property_info.get('price') or property_info.get('description')):
                    # 将提取的信息转换为文本格式
                    info_text = []
                    for key, value in property_info.items():
                        if value:
                            info_text.append(f"{key}: {value}")

                    text = " | ".join(info_text)
                    return text + f' Website: {url}'
                else:
                    # 如果所有方法都失败，尝试获取整个页面内容
                    text = soup.get_text(separator=" ").strip()
                    text = re.sub(r'\s+', ' ', text)
                    return text + f' Website: {url}'

        except Exception as e:
            print(f"获取房源详情时出错: {e}")
            # 保存错误页面以便调试
            page.screenshot(path=f"error_detail_{self.extract_property_id(url)}.png")
            return None

    def extract_property_id(self, url: str) -> str:
        """
        从ImmobilienScout24 URL中提取房源ID

        参数:
            url: 房源详情页URL

        返回:
            str: 房源ID
        """
        # 从URL中提取ID，例如 https://www.immobilienscout24.de/expose/123456789
        match = re.search(r'/expose/(\d+)', url)
        if match:
            return match.group(1)
        else:
            # 如果无法提取ID，使用URL的哈希值
            return str(hash(url))


class PropertyPlaywrightCrawler:
    """使用Playwright的房源爬虫主类"""

    def __init__(self, output_dir: str = "data", headless: bool = True, user_data_dir: str = None):
        """
        初始化爬虫

        参数:
            output_dir: 输出数据目录
            headless: 是否使用无头模式（不显示浏览器窗口）
            user_data_dir: 用户配置文件目录，用于保存会话状态
        """
        self.output_dir = output_dir
        self.headless = headless
        self.user_data_dir = user_data_dir

        # 确保输出目录存在
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # 如果未提供用户数据目录，创建一个临时目录
        if not self.user_data_dir:
            self.user_data_dir = os.path.join(output_dir, "browser_profiles")
            if not os.path.exists(self.user_data_dir):
                os.makedirs(self.user_data_dir)

        # 注册支持的爬虫
        self.scrapers = {
            "kleinanzeigen.de": KleinanzeigenPlaywrightScraper(),
            "immobilienscout24.de": ImmobilienScout24PlaywrightScraper(),
        }

        # 用于存储用户代理字符串列表
        self._user_agents = [
            # 最新的 Chrome 用户代理
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            # 最新的 Firefox 用户代理
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:123.0) Gecko/20100101 Firefox/123.0",
            # 最新的 Edge 用户代理
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
            # 德国区域的用户代理
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 (de-DE)"
        ]

    def get_random_user_agent(self) -> str:
        """获取随机用户代理字符串"""
        return random.choice(self._user_agents)

    def get_scraper_for_url(self, url: str) -> Optional[PlaywrightPropertyScraper]:
        """
        根据URL获取对应的爬虫

        参数:
            url: 网站URL

        返回:
            Optional[PlaywrightPropertyScraper]: 对应的爬虫，如果不支持则返回None
        """
        domain = urlparse(url).netloc

        # 遍历注册的爬虫，查找匹配的域名
        for domain_key, scraper in self.scrapers.items():
            if domain_key in domain:
                return scraper

        print(f"不支持的网站域名: {domain}")
        return None

    def register_scraper(self, domain: str, scraper: PlaywrightPropertyScraper):
        """
        注册新的爬虫

        参数:
            domain: 域名
            scraper: 爬虫实例
        """
        self.scrapers[domain] = scraper
        print(f"成功注册爬虫: {domain}")

    def crawl_properties(self, base_url: str, params: Dict[str, Any], pages: int = 1, proxy: str = None) -> Dict[
        str, str]:
        """
        爬取房源数据

        参数:
            base_url: 基础URL
            params: URL参数
            pages: 爬取的页数
            proxy: 代理服务器地址 (例如 "http://user:pass@proxy.example.com:8080")

        返回:
            Dict[str, str]: 房源数据字典，键为房源ID，值为网页内容
        """
        # 获取适用的爬虫
        scraper = self.get_scraper_for_url(base_url)
        if not scraper:
            print(f"找不到适用于 {base_url} 的爬虫")
            return {}

        webpage_dict = {}

        # 创建浏览器配置文件目录
        site_name = urlparse(base_url).netloc.split('.')[0]
        browser_profile_dir = os.path.join(self.user_data_dir, site_name)
        if not os.path.exists(browser_profile_dir):
            os.makedirs(browser_profile_dir)

        # 使用Playwright
        with sync_playwright() as playwright:
            # 修改：增强浏览器启动参数
            browser_args = [
                "--disable-blink-features=AutomationControlled",  # 禁用自动化控制标志
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-accelerated-2d-canvas",
                "--window-size=1920,1080",  # 设置大窗口尺寸
                # 增加更多参数以防止自动化检测
                "--disable-features=IsolateOrigins,site-per-process",
                "--disable-site-isolation-trials",
                # 模拟常见浏览器扩展
                "--enable-extensions",
                # 禁用webRTC指纹识别
                "--disable-webrtc-encryption",
                # 禁用一些可能被用于检测自动化的特性
                "--disable-web-security",
                # 使用硬件加速
                "--disable-gpu=false",
                "--use-gl=desktop"
            ]

            # 如果提供了代理，添加代理参数
            if proxy:
                browser_args.append(f"--proxy-server={proxy}")

            # 随机选择一个用户代理
            user_agent = self.get_random_user_agent()
            print(f"使用用户代理: {user_agent}")

            # 修改：使用持久性浏览器上下文而不是普通浏览器启动
            # 注意：使用persistent_context可以保存浏览器状态（登录状态、cookie等）
            print(f"使用浏览器配置文件目录: {browser_profile_dir}")

            context = playwright.chromium.launch_persistent_context(
                browser_profile_dir,
                headless=self.headless,
                args=browser_args,
                viewport={"width": 1920, "height": 1080},
                user_agent=user_agent,
                locale="de-DE",  # 设置德语区域
                timezone_id="Europe/Berlin",  # 设置德国时区
                color_scheme="no-preference",
                java_script_enabled=True,  # 确保JavaScript启用
                has_touch=False,  # 设置为桌面设备
                is_mobile=False,
                reduced_motion="no-preference",
                # 添加额外的HTTP头部
                extra_http_headers={
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                    "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
                    "Accept-Encoding": "gzip, deflate, br",
                    "Referer": "https://www.google.de/",
                    "Sec-Fetch-Dest": "document",
                    "Sec-Fetch-Mode": "navigate",
                    "Sec-Fetch-Site": "cross-site",
                    "Sec-Fetch-User": "?1",
                    "Upgrade-Insecure-Requests": "1",
                    "sec-ch-ua-platform": "\"Windows\""
                }
            )

            # 添加增强的脚本以避免webdriver检测和指纹识别
            context.add_init_script("""
                // 隐藏自动化特征
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => false
                });

                // 覆盖Playwright的检测特征
                if (window.navigator.permissions) {
                    window.navigator.permissions.query = (parameters) => {
                        return Promise.resolve({state: 'prompt'});
                    }
                }

                // 覆盖常见的navigator属性
                const newProto = navigator.__proto__;
                delete newProto.webdriver;
                navigator.__proto__ = newProto;

                // 模拟插件数量
                Object.defineProperty(navigator, 'plugins', {
                    get: () => {
                        const plugins = [
                            { name: 'Chrome PDF Plugin', filename: 'internal-pdf-viewer', description: 'Portable Document Format' },
                            { name: 'Chrome PDF Viewer', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai', description: 'Portable Document Format' },
                            { name: 'Native Client', filename: 'internal-nacl-plugin', description: 'Native Client Executable' }
                        ];

                        plugins.__proto__ = {
                            item: function(index) { return this[index]; },
                            namedItem: function(name) { return this.find(p => p.name === name); },
                            refresh: function() {},
                            length: plugins.length
                        };

                        return plugins;
                    }
                });

                // 模拟语言
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['de-DE', 'de', 'en-US', 'en']
                });

                // 修改canvas指纹
                const originalToDataURL = HTMLCanvasElement.prototype.toDataURL;
                HTMLCanvasElement.prototype.toDataURL = function(type) {
                    if (this.width === 0 && this.height === 0) {
                        return originalToDataURL.apply(this, arguments);
                    }

                    const canvas = this.cloneNode(true);
                    const ctx = canvas.getContext('2d');

                    // 添加微小噪点，每次生成不同的指纹
                    const imageData = ctx.getImageData(0, 0, canvas.width, canvas.height);
                    const pixels = imageData.data;
                    const randomPixel = () => Math.floor(Math.random() * 255);

                    // 修改部分像素
                    for (let i = 0; i < pixels.length; i += 4) {
                        // 只修改1%的像素，保持图像基本不变
                        if (Math.random() < 0.01) {
                            pixels[i] = pixels[i] < 255 ? pixels[i] + 1 : pixels[i] - 1; // R
                            pixels[i + 1] = pixels[i + 1] < 255 ? pixels[i + 1] + 1 : pixels[i + 1] - 1; // G
                            pixels[i + 2] = pixels[i + 2] < 255 ? pixels[i + 2] + 1 : pixels[i + 2] - 1; // B
                        }
                    }

                    ctx.putImageData(imageData, 0, 0);
                    return originalToDataURL.apply(canvas, arguments);
                };

                // 防止检测自动化
                // 修改navigator.connection
                if (navigator.__defineGetter__) {
                    navigator.__defineGetter__('connection', function() {
                        return {
                            effectiveType: '4g',
                            rtt: 50,
                            downlink: 10,
                            saveData: false
                        };
                    });
                }

                // 修改navigator.hardware信息
                if (navigator.__defineGetter__) {
                    navigator.__defineGetter__('hardwareConcurrency', function() {
                        return 8;
                    });
                    navigator.__defineGetter__('deviceMemory', function() {
                        return 8;
                    });
                }

                // 模拟时区
                const originalGetTimezoneOffset = Date.prototype.getTimezoneOffset;
                Date.prototype.getTimezoneOffset = function() {
                    return -120; // 对应欧洲/柏林时区（夏令时）
                };

                // 禁用Automation controller
                delete window.cdc_adoQpoasnfa76pfcZLmcfl_Array;
                delete window.cdc_adoQpoasnfa76pfcZLmcfl_Promise;
                delete window.cdc_adoQpoasnfa76pfcZLmcfl_Symbol;
            """)

            # 创建一个新页面
            page = context.new_page()

            try:
                # 获取房源链接
                print(f"开始获取房源链接...")
                links = scraper.get_property_links(page, base_url, params, pages)
                print(f"共获取到 {len(links)} 个房源链接")

                # 遍历每个链接，获取房源详情
                print(f"开始获取房源详情...")
                for i, link in enumerate(links):
                    print(f"正在处理 {i + 1}/{len(links)}: {link}")

                    # 最多尝试3次获取详情
                    max_attempts = 3
                    for attempt in range(max_attempts):
                        try:
                            # 如果不是第一次尝试，先刷新页面状态
                            if attempt > 0:
                                print(f"第{attempt + 1}次尝试获取详情...")

                                # 修改：更复杂的重试策略
                                if attempt == 1:
                                    # 仅清除cookies并重新加载
                                    context.clear_cookies()
                                    page.reload(wait_until="load", timeout=60000)
                                else:
                                    # 关闭老页面，打开新页面
                                    page.close()
                                    page = context.new_page()

                                # 等待一段时间后再试，使用递增的等待时间
                                retry_delay = 5 * (attempt + 1)
                                print(f"等待{retry_delay}秒后重试...")
                                time.sleep(retry_delay)

                            # 获取房源详情
                            property_details = scraper.get_property_details(page, link)

                            if property_details:
                                # 提取房源ID
                                property_id = scraper.extract_property_id(link)

                                # 存储结果
                                webpage_dict[property_id] = property_details
                                print(f"已保存房源 {property_id}")
                                break  # 成功获取，跳出重试循环
                            elif attempt < max_attempts - 1:
                                print(f"获取详情未返回数据，将重试...")
                                continue
                            else:
                                print(f"已尝试 {max_attempts} 次，放弃获取此房源")

                        except Exception as e:
                            print(f"尝试 {attempt + 1}/{max_attempts} 失败: {e}")
                            if attempt < max_attempts - 1:
                                print(f"将在{5 * (attempt + 1)}秒后重试...")
                            else:
                                print(f"已尝试 {max_attempts} 次，放弃获取此房源")

                    # 添加短暂延迟，避免请求过于频繁
                    if i < len(links) - 1:
                        delay = random.uniform(5, 15)  # 增加延迟时间
                        print(f"等待 {delay:.1f} 秒...")
                        time.sleep(delay)

                print(f"完成房源爬取，共获取 {len(webpage_dict)} 个有效房源")

            except Exception as e:
                print(f"爬取过程中发生错误: {e}")
                # 保存当前页面以便调试
                page.screenshot(path="error_crawl.png")

            finally:
                # 关闭浏览器上下文
                context.close()

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


# 使用示例
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='使用Playwright的房源爬虫')
    parser.add_argument('--website', type=str, choices=['kleinanzeigen', 'immoscout24'], default='immoscout24',
                        help='要爬取的网站 (kleinanzeigen 或 immoscout24)')
    parser.add_argument('--pages', type=int, default=1, help='爬取的页数')
    parser.add_argument('--output', type=str, default='property_data.json', help='输出文件名')
    parser.add_argument('--visible', action='store_true', help='显示浏览器窗口（不使用无头模式）')
    parser.add_argument('--proxy', type=str, help='代理服务器地址 (例如 http://user:pass@proxy.example.com:8080)')
    parser.add_argument('--profile-dir', type=str, help='浏览器配置文件目录，用于保存会话状态')
    args = parser.parse_args()

    # 根据选择的网站设置参数
    if args.website == 'kleinanzeigen':
        base_url = "https://www.kleinanzeigen.de/s-wohnung-mieten/aachen/"
        params = {"code": "k0c203l1921"}  # k0：表示关键字为空，c203：租房类别，l1921：亚琛
    else:  # immoscout24
        base_url = "https://www.immobilienscout24.de/Suche/de/nordrhein-westfalen/aachen/wohnung-mieten"
        params = {"price": "-900.0", "livingspace": "40.0-"}  # 示例：价格不超过900欧，面积40平以上

    # 创建爬虫实例，设置是否使用无头模式
    crawler = PropertyPlaywrightCrawler(
        headless=not args.visible,
        user_data_dir=args.profile_dir
    )

    # 爬取房源数据
    webpage_dict = crawler.crawl_properties(
        base_url=base_url,
        params=params,
        pages=args.pages,
        proxy=args.proxy
    )

    # 保存爬取结果
    crawler.save_to_json(webpage_dict, args.output)
