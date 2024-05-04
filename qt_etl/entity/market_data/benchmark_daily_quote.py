# vim set fileencoding=utf-8
"""基准市场数据
该etl目前前端暂时只用到沪深300的数据源，因此为了减少缓存大小，增加了对于沪深300的证券过滤
"""

from datetime import date, datetime
from typing import Optional, Union, List

import pyarrow as pa

from qt_common.utils import date_to_str
from qt_etl.config import get_config
from qt_etl.constants import PartitionByDateType, DEF_SEC_CSI
from qt_etl.entity.fields import Dimension, Measure
from qt_etl.entity.market_data.market_data import MarketData

__all__ = ['BenchmarkDailyQuote']

from qt_etl.entity.portfolio import resource_decorator


class BenchmarkDailyQuote(MarketData):
    """基准市场数据"""
    main_table = 'INFO_IDX_EODVALUE'
    partitioned_by_date = PartitionByDateType.month
    schema = pa.schema([
        pa.field(Dimension.TRADE_DATE, pa.string(), metadata={b'table_field': b'TRD_DATE'}),
        pa.field(Measure.CLOSE, pa.float64(), metadata={b'table_field': b'CLS_PRC'}),
        pa.field(Measure.RETURN_PERCENTAGE, pa.float64(), metadata={b'table_field': b'PCT_CHG'}),
        pa.field(Dimension.INSTRUMENT_CODE, pa.string(), metadata={b'table_field': b'IDX_CODE'}),
    ],
        metadata={
            Dimension.INSTRUMENT_CODE: '指数内部编码',
            Dimension.TRADE_DATE: '交易日期',
            Measure.CLOSE: '收盘价(点)',
            Measure.RETURN_PERCENTAGE: '涨跌幅(%)',
        }
    )
    etl_rename_dict = {
        'IDX_CODE': Dimension.INSTRUMENT_CODE,
        'TRD_DATE': Dimension.TRADE_DATE,
        'CLS_PRC': Measure.CLOSE,
        'PCT_CHG': Measure.RETURN_PERCENTAGE
    }

    @classmethod
    @resource_decorator()
    def fetch_data(cls, secu_code: Optional[Union[str, List[str]]] = None,
                   start_date: Optional[Union[datetime, date]] = None,
                   end_date: Optional[Union[datetime, date]] = None):
        sql = f"""
        SELECT TRD_DATE, 
                CLS_PRC, 
                PCT_CHG, 
                IDX_CODE 
                FROM INFO_IDX_EODVALUE
                WHERE TRD_DATE between '{start_date}' and '{end_date}' 
        """
        if secu_code and isinstance(secu_code, str):
            sql += f" AND IDX_CODE in ({secu_code})"

        # 查询sql
        df = cls.query(sql)
        # df格式化处理
        if not df.empty:
            df = df.rename(columns=cls.etl_rename_dict)
            df[Measure.CLOSE] = df[Measure.CLOSE].astype(float)
            df[Measure.RETURN_PERCENTAGE] = df[Measure.RETURN_PERCENTAGE].astype(float) / 100
            df[Dimension.TRADE_DATE] = df[Dimension.TRADE_DATE].apply(date_to_str).astype(str)
        return df

    @classmethod
    def run_etl(cls, **kwargs):
        # 过滤参数处理, 默认增加沪深300证券过滤
        secu_codes = kwargs.pop('secu_codes', get_config(
            f"etl_{cls.__name__}", "filter_secu_codes", encode=lambda x: x.split(","),
            default=DEF_SEC_CSI))
        extra = {
            "is_concurrent_query": True,
            "is_concurrent_save": True,
            "secu_codes": secu_codes
        }
        kwargs.update(extra)
        return super(BenchmarkDailyQuote, cls).run_etl(**kwargs)


if __name__ == '__main__':
    BenchmarkDailyQuote.run_etl(start_date=date(2020, 1, 1))
