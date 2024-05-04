# vim set fileencoding=utf-8
from datetime import date, datetime
from typing import Optional, Union

import pyarrow as pa

from qt_common.utils import date_to_str
from qt_etl.entity.fields import Dimension, Measure
from qt_etl.entity.market_data.market_data import MarketData
from qt_etl.entity.portfolio.comb_position import resource_decorator

__all__ = ['BondValCSI']


class BondValCSI(MarketData):
    """债券市场数据"""
    partitioned_cols = [Dimension.INSTRUMENT_CODE]
    schema = pa.schema([
        pa.field(Dimension.INSTRUMENT_CODE, pa.string(),
                 metadata={b'table_field': b'BOND_CODE', b'table_name': b'INFO_FI_VAL_CSI'}),
        pa.field(Dimension.TRADE_DATE, pa.string(),
                 metadata={b'table_field': b'TRD_DATE', b'table_name': b'INFO_FI_VAL_CSI'}),
        pa.field(Measure.YIELD, pa.float64(),
                 metadata={b'table_field': b'YIELD', b'table_name': b'INFO_FI_VAL_CSI'}),
        pa.field(Measure.DURATION, pa.float64(),
                 metadata={b'table_field': b'MODIF_DUR', b'table_name': b'INFO_FI_VAL_CSI'}),
        pa.field(Measure.VALUATION_NET_PRICE, pa.float64(),
                 metadata={b'table_field': b'NET_PRC', b'table_name': b'INFO_FI_VAL_CSI'}),
        pa.field(Measure.VALUATION_FULL_PRICE, pa.float64(),
                 metadata={b'table_field': b'FULL_PRC', b'table_name': b'INFO_FI_VAL_CSI'}),
        pa.field(Measure.INTEREST, pa.float64(),
                 metadata={b'table_field': b'PAY_INT', b'table_name': b'INFO_FI_CASHFLOW'}),
        pa.field(Measure.PRINCIPAL, pa.float64(),
                 metadata={b'table_field': b'PRCP_CASH', b'table_name': b'INFO_FI_CASHFLOW'}),
        pa.field(Measure.RCM_DIR, pa.string(),
                 metadata={b'table_field': b'RCM_DIR', b'table_name': b'INFO_FI_VAL_CSI'})
    ],
        metadata={
            Dimension.INSTRUMENT_CODE: '债券内部编码',
            Dimension.TRADE_DATE: '交易日期',
            Measure.YIELD: '收益率（%）',
            Measure.DURATION: '修正久期',
            Measure.VALUATION_NET_PRICE: '净价（元）',
            Measure.VALUATION_FULL_PRICE: '全价（元）',
            Measure.INTEREST: '偿还利息(元)',
            Measure.PRINCIPAL: '偿付本金(元)',
            Measure.RCM_DIR: '推荐方向'
        }
    )

    @classmethod
    def fetch_data(cls,
                   secu_code: str = None,
                   start_date: Optional[Union[datetime, date]] = None,
                   end_date: Optional[Union[datetime, date]] = None):
        df_bond_mkt_data = cls.fetch_bond_mkt_data(secu_code, start_date, end_date)
        return df_bond_mkt_data

    @classmethod
    def fetch_bond_mkt_data(
            cls,
            secu_code: str = None,
            start_date: Optional[Union[datetime, date]] = None,
            end_date: Optional[Union[datetime, date]] = None):
        sql = f""" select 
                    a.BOND_CODE,
                    a.TRD_DATE,
                    a.YIELD,
                    a.MODIF_DUR,
                    a.NET_PRC,
                    a.FULL_PRC,
                    b.PAY_INT,
                    b.PRCP_CASH,
                    a.RCM_DIR

                    from
                        (
                        select
                            BOND_CODE,
                            TRD_DATE,
                            YIELD,
                            MODIF_DUR,
                            NET_PRC,
                            FULL_PRC,
                            RCM_DIR
                        FROM
                            INFO_FI_VAL_CSI
                        where
                            BOND_CODE in  ({secu_code})
                            and YIELD is not null
                            ) a
                    left JOIN 
                    (
                        select
                            BOND_CODE,
                            CASH_DATE,
                            PAY_INT,
                            PRCP_CASH
                        from
                            INFO_FI_CASHFLOW
                        where
                            IS_ACT_CF = 0
                           and  BOND_CODE in  ({secu_code}) ) b on
                        a.BOND_CODE = b.BOND_CODE
                        AND a.TRD_DATE = b.CASH_DATE
                    """
        df = cls.query(sql, as_format=cls.format_bond_mkt_data)
        return df

    @classmethod
    def format_bond_mkt_data(cls, df):
        if not df.empty:
            df = df.rename(columns=cls.etl_rename_dict)
            df[Measure.PRINCIPAL] = df[Measure.PRINCIPAL].astype(float)
            df[Measure.INTEREST] = df[Measure.INTEREST].astype(float)
            df[Measure.DURATION] = df[Measure.DURATION].astype(float)
            df[Measure.VALUATION_NET_PRICE] = df[Measure.VALUATION_NET_PRICE].astype(float)
            df[Measure.VALUATION_FULL_PRICE] = df[Measure.VALUATION_FULL_PRICE].astype(float)
            df[Measure.YIELD] = df[Measure.YIELD].astype(float) / 100
            df[Dimension.TRADE_DATE] = df[Dimension.TRADE_DATE].apply(date_to_str)
            df.dropna()
        return df

    etl_rename_dict = {
        'BOND_CODE': Dimension.INSTRUMENT_CODE,
        'TRD_DATE': Dimension.TRADE_DATE,
        'YIELD': Measure.YIELD,
        'MODIF_DUR': Measure.DURATION,
        'NET_PRC': Measure.VALUATION_NET_PRICE,
        'FULL_PRC': Measure.VALUATION_FULL_PRICE,
        'PAY_INT': Measure.INTEREST,
        'PRCP_CASH': Measure.PRINCIPAL,
        'RCM_DIR': Measure.RCM_DIR
    }

    @classmethod
    @resource_decorator(secu_type="BOND")
    def run_etl(cls, secu_codes: Optional[list[str]] = None,
                start_date: Optional[Union[datetime, date]] = None,
                end_date: Optional[Union[datetime, date]] = date.today(),
                is_concurrent_query=False,
                is_concurrent_save=False,
                is_init=False):
        """增加secu_code资源过滤"""
        return super(BondValCSI, cls).run_etl(
            secu_codes=secu_codes,
            start_date=start_date,
            end_date=end_date,
            is_concurrent_query=is_concurrent_query,
            is_concurrent_save=is_concurrent_save,
            is_init=is_init)


if __name__ == '__main__':
    BondValCSI.run_etl(start_date=date(2020, 1, 1))
