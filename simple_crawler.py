import json
import os
import random
import re
import time
from typing import List, Dict, Any, Optional, Tuple
from urllib.parse import urlparse

from playwright.sync_api import sync_playwright, Page


class CoreHTMLExtractor:
    """提取房源网页中核心HTML内容的爬虫"""

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

        # 创建HTML保存目录
        self.html_dir = os.path.join(output_dir, "core_html")
        os.makedirs(self.html_dir, exist_ok=True)

        # 如果未提供用户数据目录，创建一个临时目录
        if not self.user_data_dir:
            self.user_data_dir = os.path.join(output_dir, "browser_profiles")
            if not os.path.exists(self.user_data_dir):
                os.makedirs(self.user_data_dir)

        # 用户代理列表
        self._user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:123.0) Gecko/20100101 Firefox/123.0"
        ]

    def get_random_user_agent(self) -> str:
        """获取随机用户代理字符串"""
        return random.choice(self._user_agents)

    def extract_property_id(self, url: str) -> str:
        """
        从URL中提取房源ID

        参数:
            url: 房源详情页URL

        返回:
            str: 房源ID
        """
        # 尝试从URL提取ID
        if "immobilienscout24" in url:
            match = re.search(r'/expose/(\d+)', url)
            if match:
                return match.group(1)
        elif "kleinanzeigen" in url:
            return url.split('/s-anzeige/')[-1]

        # 如果无法提取，使用URL的哈希值
        return str(hash(url))

    def get_property_links(self, page: Page, base_url: str, pages: int = 1) -> List[str]:
        """
        获取房源详情页链接

        参数:
            page: Playwright页面对象
            base_url: 基础URL
            pages: 爬取的页数

        返回:
            List[str]: 房源详情页链接列表
        """
        links = []
        current_url = base_url

        try:
            for i in range(1, pages + 1):
                print(f"正在获取第 {i} 页链接: {current_url}")

                # 导航到页面
                page.goto(current_url, wait_until="domcontentloaded", timeout=60000)

                # 等待一些时间让页面加载
                page.wait_for_timeout(random.uniform(2000, 5000))

                # 尝试处理Cookie提示
                try:
                    cookie_selectors = [
                        "button#consent-banner-btn-accept-all",
                        "button[data-testid='uc-accept-all-button']",
                        "button.consent-accept-all",
                        ".cookie-alert-extended-button-secondary",
                        "button[data-gdpr-accept-all]"
                    ]

                    for selector in cookie_selectors:
                        if page.query_selector(selector):
                            page.click(selector)
                            print(f"点击了Cookie接受按钮: {selector}")
                            page.wait_for_timeout(1000)
                            break
                except Exception as e:
                    print(f"处理Cookie提示时出错: {e}")

                # 模拟简单的滚动
                page.evaluate("window.scrollTo(0, 300)")
                page.wait_for_timeout(random.uniform(500, 1500))
                page.evaluate("window.scrollTo(0, 600)")
                page.wait_for_timeout(random.uniform(500, 1500))

                # 获取所有链接
                page_links = []

                if "immobilienscout24" in base_url:
                    # 使用JavaScript提取链接
                    page_links = page.evaluate("""() => {
                        // 尝试查找包含"/expose/"的链接
                        const allLinks = Array.from(document.querySelectorAll('a[href*="/expose/"]'));
                        return allLinks.map(link => link.href);
                    }""")
                elif "kleinanzeigen" in base_url:
                    # 获取所有房源链接
                    link_elements = page.query_selector_all("li a[href^='/s-anzeige']")
                    for element in link_elements:
                        href = element.get_attribute("href")
                        if href:
                            full_link = "https://www.kleinanzeigen.de" + href
                            page_links.append(full_link)

                # 去重
                page_links = list(set(page_links))
                links.extend(page_links)

                print(f"第{i}页获取到的链接数量: {len(page_links)}")

                # 如果有多页，寻找并点击下一页按钮
                if i < pages:
                    found_next = False

                    # 根据网站尝试不同的选择器
                    if "immobilienscout24" in base_url:
                        next_selectors = [
                            "button[data-nav-next='true']",
                            "a.pagination__nav-item--next",
                            "button[data-testid='next-page-button']"
                        ]

                        for selector in next_selectors:
                            next_button = page.query_selector(selector)
                            if next_button:
                                print(f"找到下一页按钮: {selector}")
                                next_button.click()
                                found_next = True
                                page.wait_for_timeout(random.uniform(3000, 6000))
                                current_url = page.url
                                break
                    elif "kleinanzeigen" in base_url:
                        # 构建下一页URL
                        current_url = base_url.rstrip('/') + f"/seite:{i + 1}"
                        found_next = True

                    if not found_next:
                        print("未找到下一页按钮或无法构建下一页URL，停止获取更多页面")
                        break

                # 随机延迟
                delay = random.uniform(3, 8)
                print(f"等待 {delay:.1f} 秒...")
                time.sleep(delay)

        except Exception as e:
            print(f"获取房源链接时出错: {e}")

        return links

    def extract_core_html(self, page: Page, url: str) -> Tuple[Optional[str], Optional[str]]:
        """
        提取房源详情页中的核心HTML内容

        参数:
            page: Playwright页面对象
            url: 房源详情页URL

        返回:
            Tuple[Optional[str], Optional[str]]: (核心HTML内容, 原始URL)，失败则返回(None, None)
        """
        try:
            print(f"正在获取核心HTML: {url}")

            # 导航到详情页，只等待DOM内容加载
            page.goto(url, wait_until="domcontentloaded", timeout=60000)

            # 等待短暂时间
            page.wait_for_timeout(random.uniform(2000, 4000))

            # 尝试处理Cookie提示
            try:
                cookie_selectors = ["button#consent-banner-btn-accept-all",
                                    "button[data-testid='uc-accept-all-button']"]
                for selector in cookie_selectors:
                    if page.query_selector(selector):
                        page.click(selector)
                        page.wait_for_timeout(1000)
                        break
            except Exception:
                pass

            # 简单的滚动，帮助加载更多内容
            page.evaluate("window.scrollTo(0, 300)")
            page.wait_for_timeout(1000)
            page.evaluate("window.scrollTo(0, 600)")
            page.wait_for_timeout(1000)

            core_html = None

            # 根据不同网站提取核心HTML内容
            if "immobilienscout24" in url:
                # ImmoScout24的核心内容提取
                core_html = page.evaluate("""() => {
                    // 尝试各种可能的选择器找到主要内容容器
                    let mainContent = null;
                    const selectors = [
                        '#is24-content', // 主内容区域
                        'main.main-container', // 主容器
                        'div[data-testid="is24-expose"]', // 房源详情
                        'div.flex.flex-col.gap-4.desktop\\:gap-8', // 新版布局
                        '.is24qa-objektbeschreibung' // 描述区域
                    ];

                    // 尝试每个选择器
                    for (const selector of selectors) {
                        const el = document.querySelector(selector);
                        if (el) {
                            mainContent = el;
                            break;
                        }
                    }

                    // 如果没找到主容器，尝试组合关键内容区域
                    if (!mainContent) {
                        let result = '<div class="extracted-content">';

                        // 提取标题和价格
                        const titleEl = document.querySelector('h1');
                        if (titleEl) result += titleEl.outerHTML;

                        // 提取关键信息区
                        const infoBoxes = document.querySelectorAll('.criteriagroup, .grid-item, .is24-value, .is24-ex-details');
                        infoBoxes.forEach(box => {
                            result += box.outerHTML;
                        });

                        // 提取描述
                        const descEl = document.querySelector('#expose-description, div[data-testid="description"]');
                        if (descEl) result += descEl.outerHTML;

                        // 提取地址
                        const addressEl = document.querySelector('.address-with-map-link, [data-testid="is24-expose-address"]');
                        if (addressEl) result += addressEl.outerHTML;

                        // 提取联系信息
                        const contactEl = document.querySelector('.contact-box, .contact-data, [data-testid="contactForm"]');
                        if (contactEl) result += contactEl.outerHTML;

                        result += '</div>';
                        return result;
                    }

                    // 如果找到了主容器，返回它的HTML
                    return mainContent.outerHTML;
                }""")
            elif "kleinanzeigen" in url:
                # Kleinanzeigen的核心内容提取
                core_html = page.evaluate("""() => {
                    // 尝试各种可能的选择器找到主要内容容器
                    let mainContent = null;
                    const selectors = [
                        '#viewad-content', // 主内容区域
                        '#viewad-main-container', // 主容器
                        '.addetailspage--maincolumn', // 主列
                        'article[id^="viewad-"]' // 详情文章
                    ];

                    // 尝试每个选择器
                    for (const selector of selectors) {
                        const el = document.querySelector(selector);
                        if (el) {
                            mainContent = el;
                            break;
                        }
                    }

                    // 如果没找到主容器，尝试组合关键内容区域
                    if (!mainContent) {
                        let result = '<div class="extracted-content">';

                        // 提取标题和价格
                        const titleEl = document.querySelector('h1.adTitle, h1#viewad-title');
                        if (titleEl) result += titleEl.outerHTML;

                        // 提取价格
                        const priceEl = document.querySelector('#viewad-price');
                        if (priceEl) result += priceEl.outerHTML;

                        // 提取详情表格
                        const detailsTable = document.querySelector('#viewad-details');
                        if (detailsTable) result += detailsTable.outerHTML;

                        // 提取描述
                        const descEl = document.querySelector('#viewad-description');
                        if (descEl) result += descEl.outerHTML;

                        // 提取地址
                        const addressEl = document.querySelector('#viewad-locality');
                        if (addressEl) result += addressEl.outerHTML;

                        // 提取联系信息
                        const contactEl = document.querySelector('#viewad-contact');
                        if (contactEl) result += contactEl.outerHTML;

                        result += '</div>';
                        return result;
                    }

                    // 如果找到了主容器，返回它的HTML
                    return mainContent.outerHTML;
                }""")

            if core_html and len(core_html) > 100:  # 确保提取的内容有意义
                return core_html, url
            else:
                print("未能提取到核心HTML内容或内容太短")
                return None, url

        except Exception as e:
            print(f"提取核心HTML内容时出错: {e}")
            return None, url

    def crawl_properties(self, base_url: str, pages: int = 1) -> Dict[str, Dict[str, Any]]:
        """
        爬取房源数据

        参数:
            base_url: 基础URL
            pages: 爬取的页数

        返回:
            Dict[str, Dict[str, Any]]: 房源数据字典，键为房源ID，值为包含HTML内容和元数据的字典
        """
        result_dict = {}

        # 创建浏览器配置文件目录
        site_name = urlparse(base_url).netloc.split('.')[0]
        browser_profile_dir = os.path.join(self.user_data_dir, site_name)
        if not os.path.exists(browser_profile_dir):
            os.makedirs(browser_profile_dir)

        # 使用Playwright
        with sync_playwright() as playwright:
            # 浏览器参数
            browser_args = [
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--window-size=1920,1080"
            ]

            # 随机选择用户代理
            user_agent = self.get_random_user_agent()
            print(f"使用用户代理: {user_agent}")

            # 启动浏览器，使用持久化上下文
            print(f"使用浏览器配置文件目录: {browser_profile_dir}")
            context = playwright.chromium.launch_persistent_context(
                browser_profile_dir,
                headless=self.headless,
                args=browser_args,
                viewport={"width": 1920, "height": 1080},
                user_agent=user_agent,
                locale="de-DE",
                timezone_id="Europe/Berlin"
            )

            # 创建页面
            page = context.new_page()

            try:
                # 获取房源链接
                print(f"开始获取房源链接...")
                links = self.get_property_links(page, base_url, pages)
                print(f"共获取到 {len(links)} 个房源链接")

                # 遍历每个链接获取HTML
                print(f"开始提取房源核心HTML...")
                for i, link in enumerate(links):
                    print(f"正在处理 {i + 1}/{len(links)}: {link}")

                    # 最多尝试3次
                    max_attempts = 3
                    for attempt in range(max_attempts):
                        try:
                            if attempt > 0:
                                print(f"第{attempt + 1}次尝试提取核心HTML...")
                                page.reload()
                                time.sleep(5 * (attempt + 1))

                            # 提取核心HTML
                            core_html, original_url = self.extract_core_html(page, link)

                            if core_html:
                                # 提取房源ID
                                property_id = self.extract_property_id(link)

                                # 保存HTML到文件
                                html_file_path = os.path.join(self.html_dir, f"{property_id}.html")
                                with open(html_file_path, "w", encoding="utf-8") as f:
                                    f.write(core_html)
                                print(f"已保存核心HTML到: {html_file_path}")

                                # 存储结果
                                result_dict[property_id] = {
                                    "html_file": html_file_path,
                                    "original_url": original_url,
                                    "extracted_time": time.strftime("%Y-%m-%d %H:%M:%S")
                                }
                                break  # 成功获取，跳出重试循环
                            elif attempt < max_attempts - 1:
                                print(f"提取核心HTML未返回数据，将重试...")
                                continue
                            else:
                                print(f"已尝试 {max_attempts} 次，放弃获取此房源")

                        except Exception as e:
                            print(f"尝试 {attempt + 1}/{max_attempts} 失败: {e}")
                            if attempt < max_attempts - 1:
                                print(f"将在{5 * (attempt + 1)}秒后重试...")
                            else:
                                print(f"已尝试 {max_attempts} 次，放弃获取此房源")

                    # 添加延迟
                    if i < len(links) - 1:
                        delay = random.uniform(3, 8)
                        print(f"等待 {delay:.1f} 秒...")
                        time.sleep(delay)

                print(f"完成房源核心HTML提取，共获取 {len(result_dict)} 个有效房源")

            except Exception as e:
                print(f"爬取过程中发生错误: {e}")

            finally:
                # 关闭浏览器
                context.close()

        return result_dict

    def save_to_json(self, data: Dict[str, Dict[str, Any]], filename: str) -> None:
        """
        将数据保存为JSON文件

        参数:
            data: 要保存的数据
            filename: 文件名（不含路径）
        """
        filepath = os.path.join(self.output_dir, filename)

        with open(filepath, 'w', encoding='utf-8') as json_file:
            json.dump(data, json_file, indent=4, ensure_ascii=False)
        print(f"映射数据已保存至: {filepath}")


# 使用示例
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='房源核心HTML内容提取器')
    parser.add_argument('--website', type=str, choices=['kleinanzeigen', 'immoscout24'], default='immoscout24',
                        help='要爬取的网站 (kleinanzeigen 或 immoscout24)')
    parser.add_argument('--pages', type=int, default=1, help='爬取的页数')
    parser.add_argument('--output', type=str, default='property_mapping.json', help='输出JSON映射文件名')
    parser.add_argument('--visible', action='store_true', help='显示浏览器窗口（不使用无头模式）')
    parser.add_argument('--profile-dir', type=str, help='浏览器配置文件目录')
    args = parser.parse_args()

    # 根据选择的网站设置参数
    if args.website == 'kleinanzeigen':
        base_url = "https://www.kleinanzeigen.de/s-wohnung-mieten/aachen"
    else:  # immoscout24
        base_url = "https://www.immobilienscout24.de/Suche/de/nordrhein-westfalen/aachen/wohnung-mieten"

    # 创建爬虫实例
    extractor = CoreHTMLExtractor(
        headless=not args.visible,
        user_data_dir=args.profile_dir
    )

    # 爬取房源数据
    result_dict = extractor.crawl_properties(
        base_url=base_url,
        pages=args.pages
    )

    # 保存爬取结果
    extractor.save_to_json(result_dict, args.output)
