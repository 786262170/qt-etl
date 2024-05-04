# vim set fileencoding=utf-8

from qt_etl.entity.entity_base import EntityBase


class Scenario(EntityBase):
    """情景"""
    partitioned_by_date = False
