# vim set fileencoding=utf-8
from datetime import date, datetime
from typing import Optional, Union

import pandas as pd
import pyarrow as pa

from qt_etl.entity.fields import InstrumentType, Dimension
from qt_etl.entity.instruments.instrument import Instrument
from qt_etl.utils import date_to_str

__all__ = ['FundInfo']


class FundInfo(Instrument):
    """产品基本信息"""
    main_table = 'PROD_FUNDS'
    schema = pa.schema([
        pa.field(Dimension.INSTRUMENT_CODE, pa.string(), metadata={b"table_field": b"FUND_CODE"}),
        pa.field(Dimension.INSTRUMENT_NAME, pa.string(), metadata={b"table_field": b"FUND_NAME"}),
        pa.field(Dimension.DELIST_DATE, pa.string(), metadata={b"table_field": b"FUND_NAME"}),
        pa.field(Dimension.INSTRUMENT_TYPE, pa.string())
    ],
        metadata={
            Dimension.INSTRUMENT_CODE: '产品代码',
            Dimension.INSTRUMENT_NAME: '产品名称',
            Dimension.DELIST_DATE: '退市日期',
            Dimension.INSTRUMENT_TYPE: 'instrument类型',
        }
    )

    @classmethod
    def fetch_fund_basicinfo(cls, secu_code: str = None,
                   start_date: Optional[Union[datetime, date]] = None,
                   end_date: Optional[Union[datetime, date]] = None):
        sql = """select
            FUND_CODE,
            CSNAME,
            DELIST_DATE
        from
            INFO_FUND_BASICINFO
        """
        df = cls.query(sql)
        if len(df):
            df.rename(columns=cls.etl_rename_dict, inplace=True)
            df[Dimension.DELIST_DATE] = df[Dimension.DELIST_DATE].apply(date_to_str)
        return df


    @classmethod
    def fetch_data(cls,
                   secu_code: str = None,
                   start_date: Optional[Union[datetime, date]] = None,
                   end_date: Optional[Union[datetime, date]] = None):
        sql = f"""SELECT FUND_CODE,
                    FUND_NAME
                    FROM PROD_FUNDS"""
        prod_df = cls.query(sql)
        if len(prod_df):
            prod_df.rename(columns=cls.etl_rename_dict, inplace=True)
        basicinfo_df = cls.fetch_fund_basicinfo(secu_code=secu_code, start_date=start_date, end_date=end_date)
        df = pd.concat([prod_df, basicinfo_df])
        if len(df):
            df = df.reset_index(drop=True)

            df[Dimension.INSTRUMENT_TYPE] = InstrumentType.FUND.name
        return df

    etl_rename_dict = {
        'FUND_CODE': Dimension.INSTRUMENT_CODE,
        'FUND_NAME': Dimension.INSTRUMENT_NAME,
        'CSNAME': Dimension.INSTRUMENT_NAME,
        'DELIST_DATE': Dimension.DELIST_DATE
    }

    @classmethod
    def get_name(cls, code):
        name = None
        df = cls.get_data(cond={Dimension.INSTRUMENT_CODE: [code]})
        if isinstance(df, pd.DataFrame) and len(df):
            name = df.iloc[0][Dimension.INSTRUMENT_NAME]
        return name

    partitioned_by_date = False


if __name__ == '__main__':
    FundInfo.run_etl()
    df = FundInfo.get_name('PRD000000001')
    print(df)
