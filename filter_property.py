from typing import List, Dict, Any
from gpt_client import gpt_client

import json


class Property:
    def __init__(self, id: str, data: Dict[str, Any]):
        self.id = id
        self.data = data


def load_properties_from_file(file_path: str) -> List[Property]:
    """
    从 JSON 文件加载房源数据
    :param file_path: 文件路径
    :return: 房源列表
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    return [Property(id=key, data=value) for key, value in data.items()]


def filter_properties(properties: List[Property], requirements: Dict[str, Any], top_k: int = 3) -> Dict[str, Any]:
    """
    筛选房源并根据匹配度打分，返回结构化结果。

    :param properties: 房源列表，每个房源是一个包含详细信息的字典。
    :param requirements: 用户需求的字典，例如：
                          {
                              "rent_range": [800, 1200],
                              "location": ["Charlottenburg", "Pankow"],
                              "min_area": 50,
                              "max_area": 100,
                              "rooms": 2
                          }
    :param top_k: 返回的最高得分房源数量。
    :return: 包含筛选房源的结构化结果。
    """

    def calculate_score(property_data: Dict[str, Any], user_requirements: Dict[str, Any]) -> float:
        """计算房源匹配度分数"""
        score = 0
        total_weight = 0

        # 租金范围匹配
        if "rent_range" in user_requirements:
            min_rent, max_rent = user_requirements["rent_range"]
            rent = property_data.get("rent", float('inf'))
            if min_rent <= rent <= max_rent:
                score += 50  # 完全匹配得50分
            else:
                score += max(0, 50 - abs(rent - min_rent) / (max_rent - min_rent) * 50)  # 根据偏差减分
            total_weight += 50

        # 地理位置匹配
        if "location" in user_requirements:
            desired_locations = user_requirements["location"]
            if property_data.get("location") in desired_locations:
                score += 30
            total_weight += 30

        # 面积范围匹配
        if "min_area" in user_requirements or "max_area" in user_requirements:
            area = property_data.get("area_sqm", 0)
            min_area = user_requirements.get("min_area", 0)
            max_area = user_requirements.get("max_area", float('inf'))
            if min_area <= area <= max_area:
                score += 20
            else:
                score += max(0, 20 - abs(area - min_area) / (max_area - min_area + 1) * 20)  # 偏差减分
            total_weight += 20

        # 房间数量匹配
        if "rooms" in user_requirements:
            desired_rooms = user_requirements["rooms"]
            rooms = property_data.get("rooms", 0)
            score += max(0, 10 - abs(rooms - desired_rooms) * 2)  # 每个房间偏差减2分
            total_weight += 10

        return score / total_weight if total_weight > 0 else 0

    # 计算所有房源的分数
    scored_properties = []
    for prop in properties:
        score = calculate_score(prop.data, requirements)
        scored_properties.append({
            "id": prop.id,
            "data": prop.data,
            "score": score
        })

    # 按分数排序
    scored_properties.sort(key=lambda x: x["score"], reverse=True)

    # 返回结构化结果
    # result = {
    #     "matched_properties": scored_properties[:top_k],  # 匹配的前k个房源
    #     "alternative_properties": scored_properties[top_k:]  # 剩余房源作为备选
    # }
    #
    # if not result["matched_properties"]:
    #     print("未找到完全符合需求的房源，返回最接近的结果。")

    result = {'matched_properties': scored_properties[:top_k]}

    return result

def property_evaluate_gpt_bot(user_requirement, property_content):
    content = f"""
    分析和对比如下房产信息: {property_content},
    和用户的需求信息: {user_requirement}.
    请使用轻松愉快, 且用推荐的口吻, 向用户解释为什么推荐这个房源,它有什么优缺点.
    """
    completion = gpt_client.beta.chat.completions.parse(
        model="gpt-4o-2024-08-06",
        messages=[
            {"role": "user", "content": content},
        ],
    )

    # event = completion.choices[0].message.parsed

    return completion.choices[0].message.content



def assistant_filter_properties(user_requirements: Dict[str, Any],
                                file_path: str = "property_analysis_results3.json") -> Dict[str, Any]:
    """
    集成助手功能，获取用户需求并筛选房源。

    :param user_requirements: 用户提供的筛选条件。
    :param file_path: 房源数据文件路径。
    :return: 筛选后的房源结果。
    """
    # 从文件加载房源数据
    properties = load_properties_from_file(file_path)

    # 调用筛选函数
    results = filter_properties(properties, user_requirements)

    # 打印结果，供调试
    # for property in results["matched_properties"]:
    #     print(f"筛选后的结果：", property["data"]["raw_html"], " 评分: ", property['score'])

    explaination = property_evaluate_gpt_bot(user_requirements, results["matched_properties"][0]['data'])
    print(explaination)

    return explaination


# 示例调用
if __name__ == "__main__":
    user_requirements = {
        "rent_range": [800, 1200],
        "location": ["Charlottenburg", "Pankow"],
        "min_area": 50,
        "max_area": 100,
        "rooms": 2
    }

    explaination = assistant_filter_properties(user_requirements)
