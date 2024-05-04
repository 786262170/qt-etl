# vim set fileencoding=utf-8

import json
from datetime import date, datetime
from typing import Optional, Union

import pandas as pd
import pyarrow as pa

from qt_common.utils import date_to_str
from qt_etl.entity.fields import Dimension, Measure, InstrumentType, Currency
from qt_etl.entity.market_data.market_data import MarketData
from qt_etl.constants import PartitionByDateType
from qt_etl.entity.market_data.yield_curve_cnbd_sample import curve_code_decorator


__all__ = ["IndexRate"]


class IndexRate(MarketData):
    """参考利率"""
    partitioned_by_date = PartitionByDateType.month
    name_source_table = {
        "INFO_FI_CNBD_YIELD_CURV": "CURV_CNAME",
        "INFO_FI_GLOBAL_IBOR": "CNAME",
    }
    code_source_table = {
        "INFO_FI_CNBD_YIELD_CURV": "CURV_CODE",
        "INFO_FI_GLOBAL_IBOR": "CURV_CODE",
        "INFO_FI_BNK_DL_IR": "CURV_CODE",
        "INFO_FI_IR_BASICINFO": "CURV_CODE",
    }
    trade_date_source_table = {
        "INFO_FI_CNBD_YIELD_CURV": "TRD_DATE",
        "INFO_FI_GLOBAL_IBOR": "TRD_DATE",
    }
    intr_rat_source_table = {
        "INFO_FI_BNK_DL_IR": "INTR_RAT",
        "INFO_FI_GLOBAL_IBOR": "INTR_RAT",
    }
    schema = pa.schema([
        pa.field(Dimension.INDEX, pa.string(), metadata={b'source_table': json.dumps(code_source_table).encode()}),
        pa.field(Dimension.INDEX_NAME, pa.string(), metadata={b'source_table': json.dumps(name_source_table).encode()}),
        pa.field(Dimension.TRADE_DATE, pa.string(), metadata={b'source_table': json.dumps(trade_date_source_table).encode()}),
        pa.field(Dimension.TENOR, pa.string(), metadata={b'table_field': b'STD_TERM', b'table_name': b'INFO_FI_CNBD_YIELD_CURV'}),
        pa.field(Dimension.TENOR2, pa.string(), metadata={b'table_field': b'FWD_TERM_N', b'table_name': b'INFO_FI_CNBD_YIELD_CURV'}),
        pa.field(Measure.RATE, pa.float64(), metadata={b'source_table': json.dumps(intr_rat_source_table).encode()}),
        pa.field(Dimension.CURRENCY, pa.string(), metadata={}),
        pa.field(Dimension.INSTRUMENT_TYPE, pa.string(), metadata={}),
        pa.field(Dimension.CURVE_TYPE, pa.string(), metadata={b'table_field': b'IRTYPE', b'table_name': b'INFO_PARA_INFO'}),
        pa.field(Dimension.CURVE_TYPE_NAME, pa.string(), metadata={b'table_field': b'PARA_NAME', b'table_name': b'INFO_PARA_INFO'}),
    ],
        metadata={
            Dimension.INDEX: '曲线编号',
            Dimension.INDEX_NAME: '曲线名称',
            Dimension.TRADE_DATE: '交易日期',
            Dimension.TENOR: '标准期限(年)',
            Dimension.TENOR2: '远期期限(年)',
            Measure.RATE: '收益率(%)',
        }
    )

    @classmethod
    def fetch_data(cls,
                   secu_codes: str = None,
                   start_date: Optional[Union[datetime, date]] = None,
                   end_date: Optional[Union[datetime, date]] = None):
        df_zz_yield_curve = cls.fetch_zz_yield_curve(secu_codes, start_date, end_date)
        df_deposit_loan_rate = cls.fetch_deposit_loan_rate(secu_codes, start_date, end_date)
        df_ibor_rate = cls.fetch_global_ibor_rate(secu_codes, start_date, end_date)
        res_df = pd.concat([df_zz_yield_curve, df_deposit_loan_rate, df_ibor_rate])
        if not res_df.empty:
            curve_type_df = cls.fetch_curve_type()
            res_df = res_df.merge(curve_type_df, how='left')
            res_df = res_df.reset_index(drop=True)

        return res_df

    @classmethod
    def fetch_zz_yield_curve(cls,
                             secu_codes: str = None,
                             start_date: Optional[Union[datetime, date]] = None,
                             end_date: Optional[Union[datetime, date]] = None
                             ):
        # 和JAVA保持一致，曲线取关键期限
        sql = f"""SELECT CURV_CODE,
                         CURV_CNAME,
                         TRD_DATE,
                         STD_TERM,
                         FWD_TERM_N,
                         YIELD
                    FROM INFO_FI_CNBD_YIELD_CURV
                    WHERE TRD_DATE between '{start_date}' and '{end_date}'
                        and STD_TERM in (0, 0.08, 0.17, 0.25, 0.5, 0.75, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 15, 20, 30, 40, 50)
                        and CURV_CODE in ({secu_codes})
                    """

        df = cls.query(sql)
        if not df.empty:
            df = df.rename(columns={'CURV_CODE': Dimension.INDEX,
                                    'CURV_CNAME': Dimension.INDEX_NAME,
                                    'TRD_DATE': Dimension.TRADE_DATE,
                                    'STD_TERM': Dimension.TENOR,
                                    'FWD_TERM_N': Dimension.TENOR2,
                                    'YIELD': Measure.RATE})

            df[Dimension.CURRENCY] = str(Currency.CNY).lower()
            df[Dimension.INSTRUMENT_TYPE] = InstrumentType.DEPOSIT.name
            df[Dimension.TRADE_DATE] = df[Dimension.TRADE_DATE].apply(date_to_str).astype(str)
            df[Dimension.TENOR] = df[Dimension.TENOR].apply(lambda x: f'{int(x)}Y' if x.is_integer() else f'{x}Y')
            df[Dimension.TENOR2] = df[Dimension.TENOR2].apply(lambda x: f'{int(x)}Y' if x.is_integer() else f'{x}Y')
            # df[Dimension.TENOR] = df[Dimension.TENOR].astype(str) + 'Y'
            df[Measure.RATE] = df[Measure.RATE].astype(float) / 100
            # TODO too many nan rate
            # TODO missing curve date
            df = df[~df[Measure.RATE].isna()]

        return df

    @classmethod
    def fetch_deposit_loan_rate(cls,
                                secu_codes: str = None,
                                start_date: Optional[Union[datetime, date]] = None,
                                end_date: Optional[Union[datetime, date]] = None
                                ):
        sql = f"""SELECT
                        IR_CODE,
                        CNAME,
                        CHG_START_DATE,
                        INTR_RAT
                  FROM
                        INFO_FI_BNK_DL_IR
                  WHERE CHG_START_DATE between '{start_date}' and '{end_date}'"""
        df = cls.query(sql)
        if not df.empty:
            df = df.rename(columns={
                'IR_CODE': Dimension.INDEX,
                'CNAME': Dimension.INDEX_NAME,
                'CHG_START_DATE': Dimension.TRADE_DATE,
                'INTR_RAT': Measure.RATE
            })
            df = df[~df[Dimension.TRADE_DATE].isna()]
            df[Dimension.TRADE_DATE] = df[Dimension.TRADE_DATE].apply(date_to_str)
            df[Dimension.CURRENCY] = str(Currency.CNY).lower()
            df[Dimension.INSTRUMENT_TYPE] = InstrumentType.DEPOSIT.name
            df[Measure.RATE] = df[Measure.RATE].astype(float) / 100
        return df

    @classmethod
    def fetch_global_ibor_rate(cls,
                               secu_codes: str = None,
                               start_date: Optional[Union[datetime, date]] = None,
                               end_date: Optional[Union[datetime, date]] = None
                               ):

        sql = f"""select
                    IR_CODE,
                    CNAME,
                    TRD_DATE,
                    INTR_RAT
                from
                    INFO_FI_GLOBAL_IBOR
                WHERE TRD_DATE between '{start_date}' and '{end_date}'"""
        df = cls.query(sql)
        if not df.empty:
            df = df.rename(columns={
                'IR_CODE': Dimension.INDEX,
                'CNAME': Dimension.INDEX_NAME,
                'TRD_DATE': Dimension.TRADE_DATE,
                'INTR_RAT': Measure.RATE
            })
            df = df[~df[Dimension.TRADE_DATE].isna()]
            df[Dimension.TRADE_DATE] = df[Dimension.TRADE_DATE].apply(date_to_str)
            df[Dimension.CURRENCY] = str(Currency.CNY).lower()
            df[Dimension.INSTRUMENT_TYPE] = InstrumentType.DEPOSIT.name
            df[Measure.RATE] = df[Measure.RATE].astype(float) / 100
            df[Dimension.TENOR] = df[Dimension.INDEX].map(cls.ibor_tenor_mapping)
        return df

    @classmethod
    def fetch_curve_type(cls):
        sql = """select
            INFO_FI_IR_BASICINFO.IRCODE, 
            INFO_FI_IR_BASICINFO.IRTYPE,
            INFO_PARA_INFO.PARA_NAME
        from
            INFO_FI_IR_BASICINFO
        left join INFO_PARA_INFO
        on
            INFO_FI_IR_BASICINFO.IRTYPE = INFO_PARA_INFO.para_code
            and INFO_PARA_INFO.para_obj = '031001'"""

        df = cls.query(sql)
        if not df.empty:
            df = df.rename(columns={
                    "IRCODE": Dimension.INDEX,
                    "IRTYPE": Dimension.CURVE_TYPE,
                    "PARA_NAME": Dimension.CURVE_TYPE_NAME
                })
        return df

    ibor_tenor_mapping = {
        'CN002001': '5Y',
        'SH001001': '0.0027Y',
        'SH001002': '0.0194Y',
        'SH001003': '0.0389Y',
        'SH001004': '0.0833Y',
        'SH001005': '0.25Y',
        'SH001006': '0.5Y',
        'SH001007': '0.75Y',
        'SH001008': '1Y',
        'SH002001': '1Y'
    }

    bank_dl_tenor_mapping = {
        'DL001006': '1Y'
    }

    @classmethod
    def get_shockable_factors(cls):
        df = cls.get_data()
        df = df[~df[Dimension.TENOR].isna()]
        df = df[[Dimension.INDEX, Dimension.INDEX_NAME, Dimension.TENOR,
                 Dimension.INSTRUMENT_TYPE]].drop_duplicates()
        return df



    @classmethod
    @curve_code_decorator
    def run_etl(cls, secu_codes: Optional[list[str]] = None,
                start_date: Optional[Union[datetime, date]] = None,
                end_date: Optional[Union[datetime, date]] = date.today(),
                is_concurrent_query=False,
                is_concurrent_save=False,
                is_init=False):
        return super(IndexRate, cls).run_etl(
            secu_codes=secu_codes,
            start_date=start_date,
            end_date=end_date,
            is_concurrent_query=is_concurrent_query,
            is_concurrent_save=is_concurrent_save,
            is_init=is_init)


if __name__ == '__main__':
    # IndexRate.run_etl(secu_codes=['CBD100222'], start_date=date(2021, 1, 1), end_date=date(2021, 12, 31))
    df = IndexRate.get_data(cond={Dimension.INDEX: 'CBD100222', Dimension.TRADE_DATE: '20210301'})
    print(df)

