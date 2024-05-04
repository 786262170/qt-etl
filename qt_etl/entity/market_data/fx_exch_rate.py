# vim set fileencoding=utf-8
"""汇率关系表"""
from datetime import datetime, date
from typing import Optional, Union, List

import pyarrow as pa

from qt_common import utils
from qt_common.utils import PandasMixin
from qt_etl.entity.fields import Dimension, Measure
from qt_etl.entity.market_data.market_data import MarketData

__all__ = ["FxExchRate"]


class FxExchRate(MarketData):
    """汇率关系表"""
    main_table = 'INFO_FX_EXCHRATE'
    schema = pa.schema([
        pa.field(Dimension.FX_CODE, pa.string(), metadata={b"table_field": b"FX_CODE"}),
        pa.field(Measure.EXCH_RATE, pa.float64(), metadata={b"table_field": b"EXCH_PRC"}),
        pa.field(Dimension.TRADE_DATE, pa.string(), metadata={b"table_field": b"END_DATE"}),
        pa.field("prc_type", pa.string(), metadata={b"table_field": b"PRC_TYPE"}),
        pa.field("pub_org", pa.string(), metadata={b"table_field": b"PUB_ORG"})
    ],
        metadata={
            Dimension.FX_CODE: '货币对代码',
            Measure.EXCH_RATE: '兑换价格',
            Dimension.TRADE_DATE: '日期',
            "prc_type": '价格类型',
            "pub_org": '发布机构'
        }
    )

    @classmethod
    def fetch_data(cls, secu_code: Optional[Union[str, List[str]]] = None,
                   start_date: Optional[Union[datetime, date]] = None,
                   end_date: Optional[Union[datetime, date]] = None, **kwargs):
        sql = """SELECT FX_CODE, EXCH_PRC, END_DATE, PRC_TYPE, PUB_ORG FROM INFO_FX_EXCHRATE where 
        PRC_TYPE='001' AND  PUB_ORG='001' """
        df = cls.query(sql, as_format=cls.rename)
        df[Dimension.TRADE_DATE] = df[Dimension.TRADE_DATE].apply(utils.date_to_str)
        return df

    @staticmethod
    def query_exch_rate_map(calc_date: Union[str, date]):
        """根据单个时间过滤筛选汇率表映射关系范围
        :param calc_date 交易时间
        :return: Dict[str, Dict[str, float]]
        """
        if isinstance(calc_date, date):
            calc_date = utils.date_to_str(calc_date)
        df = FxExchRate.get_data(cond={Dimension.TRADE_DATE: calc_date})
        mapper = {fx_code: group[Measure.EXCH_RATE].iloc[0] for fx_code, group in df.groupby(
            Dimension.FX_CODE)}
        return mapper

    @staticmethod
    def query_date_exch_rate_map(start_date=None, end_date=None):
        """根据时间区间筛选汇率表映射关系范围
        :param start_date: 开始时间，结束时间
        :param end_date: 开始时间，结束时间
        :return: Dict[str, Dict[str, float]]
        """
        date_df_g = FxExchRate.get_data(
            start_date=start_date, end_date=end_date,
            decoder=lambda x: PandasMixin.df_group(x, by=Dimension.TRADE_DATE))
        date_fx_rate_g = {}
        for trd_date, g in date_df_g.items():
            if trd_date not in date_fx_rate_g:
                date_fx_rate_g[trd_date] = {}
            for f, _g in g.groupby(Dimension.FX_CODE):
                date_fx_rate_g[trd_date][f] = _g[Measure.EXCH_RATE].iloc[0]
        return date_fx_rate_g


if __name__ == '__main__':
    FxExchRate.run_etl(is_init=True)
    print(FxExchRate.query_exch_rate_map("20221220"))
