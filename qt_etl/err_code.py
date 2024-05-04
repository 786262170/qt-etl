# vim set fileencoding=utf-8
# coding=utf-8
"""etl业务状态码"""

from qt_common.error import QtError


class EtlError(QtError):
    """etl异常状态码"""
    # 数据错误
    E_NOT_EXIST = (2000, "Etl not exists", "数据错误")

    # 业务错误
    ...
