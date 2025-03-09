import logging
import tiktoken
import time
import json
from typing import Dict, Any, List, Optional, Union, Tuple
from utils.property_cache import PropertyCache

# 配置日志
logger = logging.getLogger(__name__)


class GPTOptimizer:
    """
    GPT API 调用优化器，用于减少API调用成本并提高性能
    """

    def __init__(self, gpt_client, cache: PropertyCache = None, cache_enabled: bool = True):
        """
        初始化GPT优化器

        参数:
            gpt_client: OpenAI客户端
            cache: 缓存系统实例
            cache_enabled: 是否启用缓存
        """
        self.gpt_client = gpt_client
        self.cache = cache or PropertyCache()
        self.cache_enabled = cache_enabled

        # 模型定价信息 (美元/1K tokens)
        self.pricing = {
            "gpt-4o": {
                "input": 0.005,
                "output": 0.015
            },
            "gpt-4o-mini": {
                "input": 0.0015,
                "output": 0.0060
            },
            "gpt-3.5-turbo": {
                "input": 0.0005,
                "output": 0.0015
            }
        }

        logger.info("GPT优化器初始化完成")

    def count_tokens(self, text: str, model: str = "gpt-4o") -> int:
        """
        计算文本的token数量

        参数:
            text: 要计算的文本
            model: 使用的模型名称

        返回:
            int: token数量
        """
        try:
            encoding = tiktoken.encoding_for_model(model)
            tokens = encoding.encode(text)
            return len(tokens)
        except Exception as e:
            logger.warning(f"计算token数量失败: {e}，使用估算方法")
            # 粗略估计：英文约为4字符/token，中文约为1字符/token
            return len(text) // 4

    def estimate_cost(self, input_tokens: int, output_tokens: int, model: str = "gpt-4o") -> float:
        """
        估算API调用成本

        参数:
            input_tokens: 输入token数量
            output_tokens: 输出token数量
            model: 使用的模型名称

        返回:
            float: 估算成本(美元)
        """
        model_pricing = self.pricing.get(model, self.pricing["gpt-4o"])
        input_cost = (input_tokens / 1000) * model_pricing["input"]
        output_cost = (output_tokens / 1000) * model_pricing["output"]
        return input_cost + output_cost

    def select_optimal_model(self, content: str, complexity: str = "medium") -> str:
        """
        根据内容和复杂度选择最优的模型

        参数:
            content: 输入内容
            complexity: 任务复杂度 ("low", "medium", "high")

        返回:
            str: 推荐的模型名称
        """
        token_count = self.count_tokens(content)

        # 简单任务且token较少时使用更轻量的模型
        if complexity == "low" and token_count < 2000:
            return "gpt-3.5-turbo"
        elif complexity == "medium" and token_count < 3000:
            return "gpt-4o-mini"
        else:
            return "gpt-4o"

    def optimize_prompt(self, content: str, system_prompt: str) -> Tuple[str, str]:
        """
        优化提示词，减少不必要的token

        参数:
            content: 原始内容
            system_prompt: 系统提示词

        返回:
            Tuple[str, str]: (优化后的内容, 优化后的系统提示词)
        """
        # 对于房产抓取的HTML内容，可以移除无用标签和空白字符
        if "html" in content.lower() and len(content) > 5000:
            # 简化HTML，移除不必要的空白和格式
            content = ' '.join(content.split())

            # 移除常见的无用HTML区块
            content = self._remove_html_boilerplate(content)

        # 简化系统提示词，保持关键指令
        if len(system_prompt) > 500:
            system_prompt = self._simplify_system_prompt(system_prompt)

        return content, system_prompt

    def _remove_html_boilerplate(self, html_content: str) -> str:
        """移除HTML中的样板文本，保留关键内容"""
        # 此函数可以根据您的具体网站内容进行定制
        # 这里仅作为示例

        # 移除常见的无用HTML部分
        boilerplate_sections = [
            # 页脚
            r'<footer.*?</footer>',
            # 页眉
            r'<header.*?</header>',
            # 导航栏
            r'<nav.*?</nav>',
            # 广告
            r'<div[^>]*?class="[^"]*?ad[^"]*?".*?</div>',
            # 社交媒体链接
            r'<div[^>]*?class="[^"]*?social[^"]*?".*?</div>',
            # 版权信息
            r'<div[^>]*?class="[^"]*?copyright[^"]*?".*?</div>',
            # 评论区
            r'<div[^>]*?class="[^"]*?comment[^"]*?".*?</div>',
            # 相关文章
            r'<div[^>]*?class="[^"]*?related[^"]*?".*?</div>',
        ]

        import re
        cleaned_html = html_content
        for pattern in boilerplate_sections:
            cleaned_html = re.sub(pattern, '', cleaned_html, flags=re.DOTALL | re.IGNORECASE)

        return cleaned_html

    def _simplify_system_prompt(self, system_prompt: str) -> str:
        """简化系统提示词，保留关键指令"""
        # 这个方法可以根据您的需求定制

        # 如果是结构化输出的提示词，保留关键结构信息
        if "结构化输出" in system_prompt or "structural output" in system_prompt.lower():
            lines = system_prompt.split("\n")
            important_lines = [
                line for line in lines
                if any(keyword in line for keyword in [
                    "提取", "extract", "选择", "select", "解析", "parse", "WG", "landlord", "requirements",
                    "classification"
                ])
            ]
            return "\n".join(important_lines)

        # 一般情况下保留前100个字符和后100个字符
        if len(system_prompt) > 300:
            return system_prompt[:150] + "..." + system_prompt[-150:]

        return system_prompt

    def batch_process(self, items: List[Dict[str, Any]], process_func, batch_size: int = 5, delay: float = 0.5) -> List[
        Dict[str, Any]]:
        """
        批量处理多个项目，控制API调用频率

        参数:
            items: 要处理的项目列表
            process_func: 处理函数，接受一个项目并返回处理结果
            batch_size: 每批处理的项目数量
            delay: 批次间延迟(秒)

        返回:
            List[Dict[str, Any]]: 处理结果列表
        """
        results = []
        batch_count = 0

        for i in range(0, len(items), batch_size):
            batch = items[i:i + batch_size]
            batch_results = []

            for item in batch:
                result = process_func(item)
                batch_results.append(result)

            results.extend(batch_results)
            batch_count += 1

            # 添加延迟，避免API限速
            if i + batch_size < len(items):
                logger.info(f"已处理 {batch_count} 批次，共 {len(results)}/{len(items)} 项，等待 {delay} 秒...")
                time.sleep(delay)

        return results

    def analyze_property_info(
            self,
            web_content: str,
            system_prompt: str,
            model: str = "gpt-4o",
            force_refresh: bool = False,
            complexity: str = "medium"
    ) -> Dict[str, Any]:
        """
        分析房源信息，带缓存和优化

        参数:
            web_content: 房源网页内容
            system_prompt: 系统提示词
            model: 使用的模型，如果为auto则自动选择
            force_refresh: 是否强制刷新（忽略缓存）
            complexity: 任务复杂度 ("low", "medium", "high")

        返回:
            Dict[str, Any]: 分析结果
        """
        # 自动选择模型
        if model == "auto":
            model = self.select_optimal_model(web_content, complexity)

        # 优化内容和提示词
        opt_content, opt_system_prompt = self.optimize_prompt(web_content, system_prompt)

        # 计算token数量
        input_tokens = self.count_tokens(opt_content + opt_system_prompt, model)

        # 如果启用缓存且不强制刷新，尝试从缓存获取
        if self.cache_enabled and not force_refresh:
            cached_result = self.cache.get_gpt_analysis(opt_content, model)
            if cached_result:
                logger.info(f"使用缓存的分析结果，模型: {model}, 估计节省token: {input_tokens}")
                return cached_result

        try:
            # 调用API
            start_time = time.time()
            completion = self.gpt_client.beta.chat.completions.parse(
                model=model,
                messages=[
                    {"role": "system", "content": opt_system_prompt},
                    {"role": "user", "content": opt_content},
                ],
            )

            end_time = time.time()
            api_latency = end_time - start_time

            # 提取结果
            event = completion.choices[0].message.parsed
            result_dict = event.model_dump()

            # 估算输出token
            result_json = json.dumps(result_dict, ensure_ascii=False)
            output_tokens = self.count_tokens(result_json, model)

            # 计算成本
            estimated_cost = self.estimate_cost(input_tokens, output_tokens, model)

            logger.info(
                f"API调用完成，模型: {model}, 输入token: {input_tokens}, 输出token: {output_tokens}, 估计成本: ${estimated_cost:.4f}, 延迟: {api_latency:.2f}秒")

            # 缓存结果
            if self.cache_enabled:
                token_count = input_tokens + output_tokens
                self.cache.cache_gpt_analysis(opt_content, model, result_dict, token_count)

            return result_dict

        except Exception as e:
            logger.error(f"分析房源信息失败: {e}")
            # 如果失败，尝试降级到更稳定的模型
            if model == "gpt-4o" and not force_refresh:
                logger.info("尝试降级到gpt-4o-mini模型")
                return self.analyze_property_info(web_content, system_prompt, model="gpt-4o-mini", force_refresh=True)
            # 如果已经是降级模型，再尝试最低级别模型
            elif model == "gpt-4o-mini" and not force_refresh:
                logger.info("尝试降级到gpt-3.5-turbo模型")
                return self.analyze_property_info(web_content, system_prompt, model="gpt-3.5-turbo", force_refresh=True)
            raise
