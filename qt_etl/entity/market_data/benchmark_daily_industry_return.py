# vim set fileencoding=utf-8
"""INDIC_DERI_BHMARK_INDU"""
from datetime import datetime, date
from typing import Optional, Union

import pyarrow as pa

from qt_common.utils import date_to_str
from qt_etl.entity.fields import Dimension, Measure
from qt_etl.entity.market_data.market_data import MarketData

__all__ = ['BenchmarkDailyIndustryReturn']


class BenchmarkDailyIndustryReturn(MarketData):
    """基准行业衍生指标表"""
    # 查询字段重命名映射关系
    etl_rename_dict = {
        'INDU_WT_RAT': Measure.WEIGHT,
        'BUSI_DATE': Dimension.TRADE_DATE,
        'INDU_CODE_1ST': Dimension.INSTRUMENT_CODE,
        'INDU_NAME_1ST': Dimension.INSTRUMENT_NAME,
        'INDU_RETURN_RAT': Measure.RETURN_RATE,
        'BMARK_CODE': Dimension.BENCHMARK_CODE,
        'INDU_SYS_CODE': Dimension.INDUSTRY_SYS_CODE
    }
    main_table = 'INDIC_DERI_BHMARK_INDU'
    schema = pa.schema([
        pa.field(Measure.WEIGHT, pa.float64(), metadata={b"table_field": b"INDU_WT_RAT"}),
        pa.field(Dimension.TRADE_DATE, pa.string(), metadata={b"table_field": b"BUSI_DATE"}),
        pa.field(Dimension.INSTRUMENT_CODE, pa.string(), metadata={b"table_field": b"INDU_CODE_1ST"}),
        pa.field(Dimension.INSTRUMENT_NAME, pa.string(), metadata={b"table_field": b"INDU_NAME_1ST"}),
        pa.field(Measure.RETURN_RATE, pa.float64(), metadata={b"table_field": b"INDU_RETURN_RAT"}),
        pa.field(Dimension.BENCHMARK_CODE, pa.string(), metadata={b"table_field": b"BMARK_CODE"}),
        pa.field(Dimension.INDUSTRY_SYS_CODE, pa.string(), metadata={b"table_field": b"INDU_SYS_CODE"}),
    ])

    @classmethod
    def fetch_data(cls,
                   secu_code: str = None,
                   start_date: Optional[Union[datetime, date]] = None,
                   end_date: Optional[Union[datetime, date]] = None):
        sql = f"""SELECT
            INDU_WT_RAT,
            BUSI_DATE,
            INDU_CODE_1ST,
            INDU_NAME_1ST,
            INDU_RETURN_RAT,
            BMARK_CODE,
            INDU_SYS_CODE
        FROM INDIC_DERI_BHMARK_INDU where BUSI_DATE between '{start_date}' and '{end_date}' """

        df = cls.query(sql)
        if not df.empty:
            df = df.rename(columns=cls.etl_rename_dict)
            df[Dimension.TRADE_DATE] = df[Dimension.TRADE_DATE].apply(date_to_str)
        return df


if __name__ == '__main__':
    BenchmarkDailyIndustryReturn.run_etl()
