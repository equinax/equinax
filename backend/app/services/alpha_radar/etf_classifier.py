"""ETF classifier service for Alpha Radar.

Provides keyword-based ETF classification since database etf_type field is empty.
Uses ETF name patterns to classify into: broad, sector, theme, cross_border, commodity, bond.
"""

import re
from typing import Tuple, List, Optional


# ==============================================================================
# Classification Keywords
# ==============================================================================

# 宽基 ETF 关键词
BROAD_KEYWORDS = [
    "沪深300", "中证500", "中证1000", "中证2000", "中证A500", "A500", "A50",
    "上证50", "上证180", "上证380",
    "科创50", "科创100", "创业板", "创业50", "创业板50",
    "深证100", "深证成指",
    "超大盘", "大盘", "中盘", "小盘", "微盘",
    "红利", "高股息", "价值", "成长", "质量", "低波",
    "央企", "国企", "龙头", "核心资产",
    "MSCI", "富时", "恒生指数",
]

# 行业 ETF 关键词 (基于申万一级行业)
SECTOR_KEYWORDS = [
    # 金融
    "银行", "证券", "保险", "非银金融", "券商", "金融",
    # 医药
    "医药", "医疗", "生物", "医院", "中药", "制药",
    # 消费
    "食品", "饮料", "白酒", "酿酒", "家电", "家居", "消费", "零售", "商贸",
    # 汽车
    "汽车", "汽配",
    # 公用事业
    "电力", "公用事业", "燃气", "水务", "环保",
    # 周期
    "化工", "煤炭", "钢铁", "有色", "稀土", "贵金属",
    # 建筑地产
    "建材", "建筑", "地产", "房地产", "基建",
    # 交运
    "交运", "物流", "航空", "航运", "港口", "机场", "铁路", "公路",
    # 传媒
    "传媒", "游戏", "影视", "文化",
    # 军工
    "军工", "国防",
    # 农业
    "农业", "养殖", "畜牧", "农产品",
    # 纺织
    "纺织", "服装",
    # 旅游
    "旅游", "酒店", "餐饮",
    # 通信
    "通信", "电信", "5G",
    # 综合
    "综合",
]

# 赛道 ETF 关键词 (新兴主题概念)
THEME_KEYWORDS = [
    # 科技/AI
    "AI", "人工智能", "智能", "算力", "大数据", "数据",
    "芯片", "半导体", "集成电路", "光刻",
    "云计算", "云", "SAAS",
    "机器人", "自动化",
    "信创", "软件", "国产替代",
    "CPO", "光模块", "光通信", "光电",
    "物联网", "IOT",
    "网络安全", "安全",
    "计算机", "科技", "电子", "互联网",
    # 新能源
    "新能源", "光伏", "储能", "锂电", "电池",
    "智能车", "电动车", "智能驾驶", "无人驾驶", "新能源车",
    "风电", "风能",
    "氢能", "燃料电池",
    "碳中和", "绿电", "清洁能源",
    # 先进制造
    "商业航天", "航天", "卫星", "北斗",
    "先进制造", "高端装备", "工业母机",
    # 生命科学 (注意: 生物在行业优先)
    "创新药", "生物科技", "基因", "CXO",
    "医美", "养老",
    # 其他主题
    "元宇宙", "VR", "AR",
    "区块链", "数字货币",
    "ESG", "碳交易",
]

# 跨境 ETF 关键词
CROSS_BORDER_KEYWORDS = [
    "港股", "恒生", "恒科", "恒生科技", "恒生医疗", "恒生互联", "H股",
    "纳指", "纳斯达克", "标普", "标普500", "美股",
    "日经", "日本", "东证",
    "德国", "德DAX", "法国", "法CAC",
    "越南", "印度", "东南亚", "新兴市场",
    "QDII", "跨境", "海外",
]

# 商品 ETF 关键词
COMMODITY_KEYWORDS = [
    "黄金", "白银", "原油", "石油",
    "豆粕", "有色金属", "铜", "铝", "锌",
    "能源化工", "农产品期货", "商品",
]

# 债券 ETF 关键词
BOND_KEYWORDS = [
    "国债", "国开债", "政金债",
    "信用债", "城投", "公司债", "企业债",
    "可转债", "转债",
    "短融", "利率债",
    "债券", "债ETF", "债",
]


# ==============================================================================
# Name Simplification Patterns
# ==============================================================================

# 需要移除的基金公司后缀
FUND_COMPANY_SUFFIXES = [
    "ETF华夏", "ETF易方达", "ETF南方", "ETF广发", "ETF博时",
    "ETF工银", "ETF建信", "ETF招商", "ETF华安", "ETF嘉实",
    "ETF汇添富", "ETF富国", "ETF国泰", "ETF华宝", "ETF平安",
    "ETF银华", "ETF天弘", "ETF鹏华", "ETF景顺", "ETF兴业",
    "ETF中欧", "ETF大成", "ETF交银", "ETF浦银", "ETF海富通",
    "ETF中银", "ETF农银", "ETF诺安", "ETF方正",
    "ETF联接", "ETF指数", "ETF增强", "ETF优选",
    "LOF", "QDII",
]

# ETF 名称简化规则
NAME_SIMPLIFY_PATTERNS = [
    # 移除 ETF 后缀和基金公司名
    (r"ETF[A-Za-z\u4e00-\u9fa5]*$", ""),
    # 移除括号内容
    (r"\([^)]*\)", ""),
    (r"（[^）]*）", ""),
]


class ETFClassifier:
    """
    ETF classification engine based on name keyword matching.

    Classification priority (higher = checked first):
    1. cross_border (跨境) - highest priority to avoid misclassifying "港股科技"
    2. commodity (商品)
    3. bond (债券)
    4. broad (宽基) - before sector/theme to avoid "科创50" → "科技"
    5. sector (行业) - before theme per user preference
    6. theme (赛道)
    """

    @staticmethod
    def classify(name: str) -> Tuple[str, str]:
        """
        Classify an ETF by name.

        Args:
            name: Full ETF name (e.g., "沪深300ETF华夏")

        Returns:
            Tuple of (category, sub_category)
            - category: broad, sector, theme, cross_border, commodity, bond, other
            - sub_category: Simplified name representing the sub-category
        """
        if not name:
            return ("other", "未分类")

        # 1. Cross-border (highest priority)
        match = ETFClassifier._match_keywords(name, CROSS_BORDER_KEYWORDS)
        if match:
            return ("cross_border", ETFClassifier._simplify_name(name, match))

        # 2. Commodity
        match = ETFClassifier._match_keywords(name, COMMODITY_KEYWORDS)
        if match:
            return ("commodity", ETFClassifier._simplify_name(name, match))

        # 3. Bond
        match = ETFClassifier._match_keywords(name, BOND_KEYWORDS)
        if match:
            return ("bond", ETFClassifier._simplify_name(name, match))

        # 4. Broad-based (before sector/theme to avoid "科创50" → "科技")
        match = ETFClassifier._match_keywords(name, BROAD_KEYWORDS)
        if match:
            return ("broad", ETFClassifier._simplify_name(name, match))

        # 5. Sector (行业优先)
        match = ETFClassifier._match_keywords(name, SECTOR_KEYWORDS)
        if match:
            return ("sector", ETFClassifier._simplify_name(name, match))

        # 6. Theme
        match = ETFClassifier._match_keywords(name, THEME_KEYWORDS)
        if match:
            return ("theme", ETFClassifier._simplify_name(name, match))

        # 7. Default
        return ("other", ETFClassifier._simplify_name(name, None))

    @staticmethod
    def _match_keywords(name: str, keywords: List[str]) -> Optional[str]:
        """
        Find the first matching keyword in the name.

        Args:
            name: ETF name to search
            keywords: List of keywords to match

        Returns:
            The matched keyword, or None if no match
        """
        name_upper = name.upper()
        for keyword in keywords:
            if keyword.upper() in name_upper:
                return keyword
        return None

    @staticmethod
    def _simplify_name(name: str, matched_keyword: Optional[str]) -> str:
        """
        Simplify ETF name to a clean sub-category label.

        Args:
            name: Full ETF name
            matched_keyword: The keyword that matched

        Returns:
            Simplified name (e.g., "沪深300ETF华夏" → "沪深300")
        """
        if matched_keyword:
            # Use the matched keyword as the base
            simplified = matched_keyword
        else:
            # Start with the full name
            simplified = name

        # Apply simplification patterns
        for pattern, replacement in NAME_SIMPLIFY_PATTERNS:
            simplified = re.sub(pattern, replacement, simplified)

        # Remove fund company suffixes
        for suffix in FUND_COMPANY_SUFFIXES:
            simplified = simplified.replace(suffix, "")

        # Clean up whitespace
        simplified = simplified.strip()

        return simplified if simplified else name

    @staticmethod
    def get_category_label(category: str) -> str:
        """
        Get Chinese label for a category.

        Args:
            category: Category key (broad, sector, theme, etc.)

        Returns:
            Chinese label (宽基, 行业, 赛道, etc.)
        """
        labels = {
            "broad": "宽基",
            "sector": "行业",
            "theme": "赛道",
            "cross_border": "跨境",
            "commodity": "商品",
            "bond": "债券",
            "other": "其他",
        }
        return labels.get(category, "其他")

    @staticmethod
    def get_all_categories() -> List[Tuple[str, str]]:
        """
        Get all category keys and labels in display order.

        Returns:
            List of (key, label) tuples
        """
        return [
            ("broad", "宽基"),
            ("sector", "行业"),
            ("theme", "赛道"),
            ("cross_border", "跨境"),
            ("commodity", "商品"),
            ("bond", "债券"),
        ]
