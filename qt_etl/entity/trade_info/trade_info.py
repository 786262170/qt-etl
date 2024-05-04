# vim set fileencoding=utf-8

from qt_etl.entity.entity_base import EntityBase


class TradeInfo(EntityBase):
    """交易数据"""
    partitioned_by_date = False
