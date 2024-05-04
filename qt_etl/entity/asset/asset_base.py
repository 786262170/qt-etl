# vim set fileencoding=utf-8

from qt_etl.entity.entity_base import EntityBase


class Asset(EntityBase):
    """资产分类"""
    partitioned_by_date = False
