# vim set fileencoding=utf-8
from qt_etl.entity.entity_base import EntityBase


class Factor(EntityBase):
    """多级因子"""
    partitioned_by_date = False
