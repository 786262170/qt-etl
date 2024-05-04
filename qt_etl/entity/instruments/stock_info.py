# vim set fileencoding=utf-8

from datetime import date, datetime
from typing import Optional, Union

import pandas as pd
import pyarrow as pa

from qt_common.utils import date_to_str
from qt_etl.entity.fields import InstrumentType, Dimension
from qt_etl.entity.instruments.instrument import Instrument

__all__ = ['StockInfo']


class StockInfo(Instrument):
    """股票基本信息"""
    main_table = 'INFO_STK_BASICINFO'
    schema = pa.schema([
        pa.field(Dimension.INSTRUMENT_CODE, pa.string(), metadata={b"table_field": b"STK_CODE"}),
        pa.field(Dimension.SYMBOL, pa.string(), metadata={b"table_field": b"SYMBOL"}),
        pa.field(Dimension.ISSUER_CODE, pa.string(), metadata={b"table_field": b"PARTY_CODE"}),
        pa.field(Dimension.LIST_STATUS, pa.string(), metadata={b"table_field": b"LIST_STATUS"}),
        pa.field(Dimension.LIST_DATE, pa.string(), metadata={b"table_field": b"LIST_DATE"}),
        pa.field(Dimension.DELIST_DATE, pa.string(), metadata={b"table_field": b"DELIST_DATE"}),
        pa.field(Dimension.EXCHANGE_CODE, pa.string(), metadata={b"table_field": b"EXCH_CODE"}),
        pa.field(Dimension.INSTRUMENT_TYPE, pa.string(), metadata={b"table_field": b""}),
        pa.field(Dimension.INDEX, pa.string(), metadata={b"table_field": b"LIST_BRD"}),
        pa.field(Dimension.INDEX_NAME, pa.string(), metadata={b"table_field": b"LIST_BRD"}),
        pa.field(Dimension.SEC_TYPE, pa.string(), metadata={b"table_field": b"SEC_TYPE"}),
    ],
        metadata={
            Dimension.INSTRUMENT_CODE: '股票内部编码',
            Dimension.SYMBOL: '证券代码',
            Dimension.ISSUER_CODE: '主体编码',
            Dimension.LIST_STATUS: '上市状态',
            Dimension.LIST_DATE: '上市日期',
            Dimension.DELIST_DATE: '退市日期',
            Dimension.EXCHANGE_CODE: '所属交易所编码',
            Dimension.INSTRUMENT_TYPE: '产品类型',
            Dimension.INDEX: '所属板块编码',
            Dimension.INDEX_NAME: '所属板块名称',
        }
    )

    @classmethod
    def fetch_data(cls,
                   secu_code: str = None,
                   start_date: Optional[Union[datetime, date]] = None,
                   end_date: Optional[Union[datetime, date]] = None):
        sql = f"""SELECT STK_CODE,
                    SYMBOL,
                    PARTY_CODE,
                    LIST_STATUS,
                    LIST_DATE,
                    DELIST_DATE,
                    EXCH_CODE,
                    LIST_BRD,
                    SEC_TYPE
                    FROM INFO_STK_BASICINFO"""
        df = cls.query(sql)

        df[Dimension.INDEX] = df['LIST_BRD'].map({
            '001': 'SEC026261761',
            '003': 'SEC024022199',
            '004': 'SEC045336986'
        })
        df[Dimension.INDEX_NAME] = df['LIST_BRD'].map({
            '001': '上证综指',
            '003': '创业板指数',
            '004': '上证科创板50成份指数'
        })
        df[Dimension.INDEX].fillna('', inplace=True)
        df[Dimension.INDEX_NAME].fillna('', inplace=True)
        df.drop(columns=['LIST_BRD'], inplace=True)
        df.rename(columns={'STK_CODE': Dimension.INSTRUMENT_CODE,
                           'SYMBOL': Dimension.SYMBOL,
                           'PARTY_CODE': Dimension.ISSUER_CODE,
                           'LIST_STATUS': Dimension.LIST_STATUS,
                           'LIST_DATE': Dimension.LIST_DATE,
                           'DELIST_DATE': Dimension.DELIST_DATE,
                           'EXCH_CODE': Dimension.EXCHANGE_CODE,
                           'SEC_TYPE': Dimension.SEC_TYPE},
                  inplace=True)

        df[Dimension.LIST_STATUS] = df[Dimension.LIST_STATUS].apply(str)
        df[Dimension.LIST_DATE] = df[Dimension.LIST_DATE].apply(
            lambda x: date_to_str(x) if not pd.isnull(x) else '')
        df[Dimension.DELIST_DATE] = df[Dimension.DELIST_DATE].apply(
            lambda x: date_to_str(x) if not pd.isnull(x) else '')
        df[Dimension.INSTRUMENT_TYPE] = InstrumentType.STOCK.name
        # TODO 添加列：是否停牌 所属指数
        return df


if __name__ == '__main__':
    StockInfo.run_etl(is_init=True)
