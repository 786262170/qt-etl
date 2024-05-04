from datetime import date, datetime
from typing import Optional, Union

import pyarrow as pa

from qt_etl.entity.fields import InstrumentType, Dimension
from qt_etl.entity.instruments.instrument import Instrument

__all__ = ['StockIndexInfo']


class StockIndexInfo(Instrument):
    """股票指数信息"""
    main_table = 'INFO_IDX_BASICINFO'
    schema = pa.schema([
        pa.field(Dimension.INSTRUMENT_CODE, pa.string(), metadata={b"table_field": b"IDX_CODE"}),
        pa.field(Dimension.INSTRUMENT_NAME, pa.string(), metadata={b"table_field": b"CNAME"}),
        pa.field(Dimension.SYMBOL, pa.string(), metadata={b"table_field": b"SYMBOL"}),
        pa.field(Dimension.INSTRUMENT_TYPE, pa.string(), metadata={b"table_field": b""}),
    ],
        metadata={
            Dimension.INSTRUMENT_CODE: '指数名称',
            Dimension.INSTRUMENT_NAME: '指数内部编码',
            Dimension.SYMBOL: '指数代码',
        }
    )

    @classmethod
    def fetch_data(cls,
                   secu_code: str = None,
                   start_date: Optional[Union[datetime, date]] = None,
                   end_date: Optional[Union[datetime, date]] = None):
        sql = f"""SELECT
                    IDX_CODE,
                    CNAME,
                    SYMBOL
                FROM
                    INFO_IDX_BASICINFO"""
        df = cls.query(sql)
        df.rename(columns=cls.etl_rename_dict, inplace=True)
        df[Dimension.INSTRUMENT_TYPE] = InstrumentType.STOCK_INDEX.name
        return df

    etl_rename_dict = {
        'IDX_CODE': Dimension.INSTRUMENT_CODE,
        'CNAME': Dimension.INSTRUMENT_NAME,
        'SYMBOL': Dimension.SYMBOL
    }

    @classmethod
    def get_code(cls, name, symbol):
        code = None
        cond = {
            Dimension.INSTRUMENT_NAME: [name],
            Dimension.SYMBOL: [symbol]
        }
        df = cls.get_data(cond=cond)
        if len(df):
            code = df.iloc[0][Dimension.INSTRUMENT_CODE]

        return code


if __name__ == '__main__':
    StockIndexInfo.run_etl()
