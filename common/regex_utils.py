import re
from typing import Optional

class RegexPatterns:
    """正则表达式模式集合"""
    UUID = r'([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})'
    PHONE = r'^1[3-9]\d{9}$'

class RegexHelper:
    @staticmethod
    def extract_uuid(text: str) -> Optional[str]:
        """从文本中提取UUID"""
        match = re.search(RegexPatterns.UUID, text, re.IGNORECASE)
        return match.group(1) if match else None