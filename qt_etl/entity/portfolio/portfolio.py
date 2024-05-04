# vim set fileencoding=utf-8

from qt_etl.entity.entity_base import EntityBase


class Portfolio(EntityBase):
    """组合持仓"""
    partitioned_by_date = False
