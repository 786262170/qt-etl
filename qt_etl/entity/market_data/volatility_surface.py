# coding=utf8
"""
仅Numerix应用会用到
"""
from datetime import date, datetime
from typing import Optional, Union, List

import pyarrow as pa

from qt_common.utils import date_to_str
from qt_etl.entity.fields import Dimension, Measure
from qt_etl.entity.market_data.market_data import MarketData

__all__ = ["VolatilitySurface"]


class VolatilitySurface(MarketData):
    """曲面数据"""

    main_table = 'OPTION_VOLATILITY_SURFACE'
    schema = pa.schema([
        pa.field(Dimension.INSTRUMENT_CODE, pa.string(), metadata={b'table_field': b'ASSET_ID'}),
        pa.field(Dimension.TRADE_DATE, pa.string(), metadata={b'table_field': b'DATE'}),
        pa.field(Dimension.VOLATILITY_TYPE, pa.string(), metadata={b'table_field': b'VOLATILITY_TYPE'}),
        pa.field(Measure.VOLATILITY, pa.float64(), metadata={b'table_field': b'VOLATILITY'}),
        pa.field(Dimension.TENOR, pa.float64(), metadata={b'table_field': b'TENOR'}),
        pa.field(Dimension.FORWARD_TENOR, pa.float64(), metadata={b'table_field': b'FORWARD_TENOR'}),
        pa.field(Dimension.STRIKE_PRICE, pa.float64(), metadata={b'table_field': b'STRIKE_PRICE'}),
        pa.field(Dimension.MONEYNESS, pa.string(), metadata={b'table_field': b'MONEYNESS'}),
        pa.field(Dimension.INSTRUMENT_NAME, pa.string(), metadata={b'table_field': b'ASSET_DESCRIPTION'})

    ],
        metadata={
            Dimension.INSTRUMENT_CODE: '币对编码',
            Dimension.TRADE_DATE: '交易时间',
            Dimension.VOLATILITY_TYPE: '波动率类型',
            Measure.VOLATILITY: '波动率',
            Dimension.TENOR: '原始期限',
            Dimension.FORWARD_TENOR: '远期期限',
            Dimension.STRIKE_PRICE: '执行价格',
            Dimension.MONEYNESS: '货币性',
            Dimension.INSTRUMENT_NAME: '资产描述',
        }
    )

    @classmethod
    def fetch_data(cls, secu_code: Optional[Union[str, List[str]]] = None,
                   start_date: Optional[Union[datetime, date]] = None,
                   end_date: Optional[Union[datetime, date]] = None):
        sql = f"""SELECT
            ASSET_ID,
            `DATE`,
            VOLATILITY_TYPE,
            VOLATILITY,
            TENOR,
            FORWARD_TENOR,
            STRIKE_PRICE,
            MONEYNESS,
            ASSET_DESCRIPTION
        FROM
            OPTION_VOLATILITY_SURFACE"""
        df = cls.query(sql, session='info', as_format=cls.rename)
        if not df.empty:
            df[Dimension.TRADE_DATE] = df[Dimension.TRADE_DATE].apply(date_to_str)
        return df


if __name__ == '__main__':
    df = VolatilitySurface.run_etl(is_init=True)
