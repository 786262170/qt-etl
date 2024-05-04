
# vim set fileencoding=utf-8
from datetime import date, datetime
from typing import Optional, Union

import pandas as pd
import pyarrow as pa

from qt_etl.entity.fields import InstrumentType, Dimension
from qt_etl.entity.instruments.instrument import Instrument

__all__ = ['BenchMarkInfo']


class BenchMarkInfo(Instrument):
    """基准标记信息"""
    main_table = 'PERFORMANCE_BENCHMARKS'
    schema = pa.schema([
        pa.field(Dimension.INSTRUMENT_CODE, pa.string(),
                 metadata={b"table_field": b"PERFORMANCE_BENCHMARK_CODE"}),
        pa.field(Dimension.INSTRUMENT_NAME, pa.string(),
                 metadata={b"table_field": b"PERFORMANCE_BENCHMARK_NAME"}),
        pa.field(Dimension.INSTRUMENT_TYPE, pa.string(), metadata={}),
    ], metadata={
        Dimension.INSTRUMENT_CODE: '业绩基准代码',
        Dimension.INSTRUMENT_NAME: '业绩基准名称',
        Dimension.INSTRUMENT_TYPE: 'INSTRUMENT_TYPE',
    })

    @classmethod
    def fetch_data(cls,
                   secu_code: str = None,
                   start_date: Optional[Union[datetime, date]] = None,
                   end_date: Optional[Union[datetime, date]] = None):
        sql = f"""SELECT PERFORMANCE_BENCHMARK_CODE,
                            PERFORMANCE_BENCHMARK_NAME
                            FROM PERFORMANCE_BENCHMARKS"""
        df = cls.query(sql)
        df.rename(columns=cls.etl_rename_dict, inplace=True)
        df[Dimension.INSTRUMENT_TYPE] = InstrumentType.BENCH_MARK.name
        return df

    etl_rename_dict = {
        'PERFORMANCE_BENCHMARK_CODE': Dimension.INSTRUMENT_CODE,
        'PERFORMANCE_BENCHMARK_NAME': Dimension.INSTRUMENT_NAME,
    }

    @classmethod
    def get_name(cls, code):
        df = cls.get_data(cond={Dimension.INSTRUMENT_CODE: code})
        if isinstance(df, pd.DataFrame) and len(df):
            name = df.iloc[0][Dimension.INSTRUMENT_NAME]
        return name


if __name__ == '__main__':
    BenchMarkInfo.run_etl()