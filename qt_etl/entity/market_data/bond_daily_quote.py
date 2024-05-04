# vim set fileencoding=utf-8
"""INFO_FI_EODPRICE"""
from datetime import datetime, date
from typing import Optional, Union

import pyarrow as pa

from qt_common.utils import date_to_str
from qt_etl.entity.fields import Dimension, Measure
from qt_etl.entity.market_data.market_data import MarketData
from qt_etl.entity.portfolio.comb_position import resource_decorator

__all__ = ['BondDailyQuote']


class BondDailyQuote(MarketData):
    """中国债券日行情表"""
    # 查询字段重命名映射关系
    etl_rename_dict = {
        'BOND_CODE': Dimension.INSTRUMENT_CODE,
        'TRD_DATE': Dimension.TRADE_DATE,
        'VOL': Measure.VOLUME,
        'AMT_NET': Measure.TRADE_AMOUNT,
        'HI_NET_PRC': Measure.HIGH,
        'LO_NET_PRC': Measure.LOW,
        'TURN_RAT': Measure.TURNOVER
    }
    # 字段格式类型映射关系
    as_type_dict = {
        'BOND_CODE': str,
        'VOL': float,
        'AMT_NET': float,
        'HI_NET_PRC': float,
        'LO_NET_PRC': float,
        'TURN_RAT': float
    }

    main_table = 'INFO_FI_EODPRICE'
    schema = pa.schema([
        pa.field(Dimension.INSTRUMENT_CODE, pa.string(), metadata={b"table_field": b"BOND_CODE"}),
        pa.field(Dimension.TRADE_DATE, pa.string(), metadata={b"table_field": b"TRD_DATE"}),
        pa.field(Measure.VOLUME, pa.float64(), metadata={b"table_field": b"VOL"}),
        pa.field(Measure.TRADE_AMOUNT, pa.float64(), metadata={b"table_field": b"AMT_NET"}),
        pa.field(Measure.HIGH, pa.float64(), metadata={b"table_field": b"HI_NET_PRC"}),
        pa.field(Measure.LOW, pa.float64(), metadata={b"table_field": b"LO_NET_PRC"}),
        pa.field(Measure.TURNOVER, pa.float64(), metadata={b"table_field": b"TURN_RAT"}),
    ])

    @classmethod
    def fetch_data(cls,
                   secu_code: str = None,
                   start_date: Optional[Union[datetime, date]] = None,
                   end_date: Optional[Union[datetime, date]] = None):
        sql = f"""select BOND_CODE,
                        TRD_DATE,
                        VOL,
                        AMT_NET,
                        HI_NET_PRC,
                        LO_NET_PRC,
                        TURN_RAT from 
                        INFO_FI_EODPRICE where TRD_DATE between '{start_date}' and '{end_date}' """

        if secu_code:
            sql += f""" and BOND_CODE in ({secu_code})"""
        df = cls.query(sql, as_format=cls.format_bond_market_data)
        return df

    @classmethod
    def format_bond_market_data(cls, df):
        if len(df):
            df = df.astype(cls.as_type_dict)
            df = df.rename(columns=cls.etl_rename_dict)
            df[Dimension.TRADE_DATE] = df[Dimension.TRADE_DATE].apply(date_to_str)
            # 针对TRD_DATE, BOND_CODE字段去重, 默认保留first的记录
            df = df.drop_duplicates(subset=[Dimension.TRADE_DATE, Dimension.INSTRUMENT_CODE], keep="first")
        return df

    @classmethod
    @resource_decorator(secu_type="BOND")
    def run_etl(cls, **extra_kwargs):
        extra_kwargs["is_concurrent_query"] = True
        return super(BondDailyQuote, cls).run_etl(**extra_kwargs)


if __name__ == '__main__':
    BondDailyQuote.run_etl()
