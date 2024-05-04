# vim set fileencoding=utf-8
"""entity utils"""
from datetime import date, datetime
from typing import Union, Dict

from qt_common.protoc.db.event import EventStatus
from qt_common.utils import str_to_date, date_to_str
from qt_etl.constants import PartitionByDateType
from qt_etl.entity.fields import Currency


def deal_date(trade_date: Union[str, date, datetime],
              date_type=PartitionByDateType.month):
    """日期字符串处理"""
    trade_date = str_to_date(trade_date)
    if date_type == PartitionByDateType.month:
        return date_to_str(trade_date, fmt="%Y%m")
    elif date_type == PartitionByDateType.year:
        return str(trade_date.year)
    elif date_type == PartitionByDateType.quarter:
        quarter = int((trade_date.month - 1) / 3) + 1
        return f'{trade_date.year}{quarter}'
    else:
        raise RuntimeError('deal_date type参数有误')


def is_completed(event, end_date: Union[str, date] = None):
    """检查etl模型event状态s

    :param event: EventModel<id=12,...>
    :param end_date: 结束日期
    :return: bool 是否COMP
    """
    if event:
        if event.event_status == EventStatus.COMP:
            if end_date:
                if isinstance(end_date, str):
                    end_date = str_to_date(end_date)
                if event.business_date >= end_date:
                    return True
                else:
                    return False
    return False


class CurrExchRateTools:
    """币种汇率转换计算倍数"""

    @staticmethod
    def cny_to_other_cuur(price: float, exch_rate: float, reverse_rate: float) -> float:
        """人民币转换其他币种倍数

        :param price: 单位系数
        :param exch_rate: 汇率 人民币/其他币种
        :param reverse_rate: 汇率 其他币种/人民币
        :return:
        """
        if not any([exch_rate, reverse_rate]):
            return price
        elif exch_rate:
            return price / exch_rate
        else:
            return price * reverse_rate

    @staticmethod
    def other_cuur_to_cny(price: float, exch_rate: float, reverse_rate: float) -> float:
        """其他币种转换人民币倍数

        :param price: 单位系数
        :param exch_rate: 汇率 其他币种/人民币
        :param reverse_rate: 汇率 人民币/其他币种
        :return:
        """
        if not any([exch_rate, reverse_rate]):
            return price
        elif exch_rate:
            return price * exch_rate
        else:
            return price / reverse_rate

    @staticmethod
    def calc_curr_exch_rate(req_curr: str, self_cuur: str,
                            exch_rate_map: Dict[str, float]) -> float:

        """汇率换算倍数

        :param req_curr: 请求币种
        :param self_cuur: 持仓币种
        :param exch_rate_map: 汇率映射map
        :return:
        """
        value = float(1)
        if not self_cuur or req_curr == self_cuur:
            # 取不到，或者相同默认本币数值
            return value
        elif req_curr == Currency.CNY.value:
            # 其他币种转换为人民币
            exch_rate: float = exch_rate_map.get(self_cuur + req_curr)
            reverse_rate: float = exch_rate_map.get(req_curr + self_cuur)
            return CurrExchRateTools.other_cuur_to_cny(value, exch_rate, reverse_rate)
        elif self_cuur == Currency.CNY.value:
            # 人民币转换为其他币种
            exch_rate: float = exch_rate_map.get(req_curr + self_cuur)
            reverse_rate: float = exch_rate_map.get(self_cuur + req_curr)
            return CurrExchRateTools.cny_to_other_cuur(value, exch_rate, reverse_rate)
        else:
            # 其他币种之间的转换：1本身币种转换为人民币 2人民币再转成其他币种
            self_exch_rate: float = exch_rate_map.get(self_cuur + Currency.CNY.value)
            self_reverse_exch_rate: float = exch_rate_map.get(Currency.CNY.value + self_cuur)
            s_value = CurrExchRateTools.other_cuur_to_cny(value, self_exch_rate, self_reverse_exch_rate)

            req_exch_rate: float = exch_rate_map.get(req_curr + Currency.CNY.value)
            req_reverse_exch_rate: float = exch_rate_map.get(Currency.CNY.value + req_curr)
            return CurrExchRateTools.cny_to_other_cuur(s_value, req_exch_rate, req_reverse_exch_rate)
