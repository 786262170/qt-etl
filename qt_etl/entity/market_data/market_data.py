# vim set fileencoding=utf-8

from qt_etl.entity.entity_base import EntityBase


class MarketData(EntityBase):
    """市场数据"""
    partitioned_by_date = None
