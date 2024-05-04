# vim set fileencoding=utf-8
"""constants module"""

from enum import Enum


class PartitionByDateType(Enum):
    year = 'year'
    month = 'month'
    day = 'day'
    quarter = 'quarter'


DEF_SEC_CSI = [
    'SEC024342013',  # 沪深300
    'SEC023059609',  # 中证
    'SEC023392778',  # 上证50
    'SEC024022199',  # 创业板
    'SEC045336986'  # 科创板
]
