# vim set fileencoding=utf-8
"""定制化市场数据"""
from datetime import datetime, date
from typing import Optional, Union

import numpy as np
import pyarrow as pa

from qt_common.utils import date_to_str
from qt_etl.entity.fields import Dimension, Measure
from qt_etl.entity.market_data.market_data import MarketData
from qt_etl.entity.portfolio.comb_position import resource_decorator

__all__ = ["BondValCNBD"]


class BondValCNBD(MarketData):
    """中债登债券估值"""
    partitioned_cols = [Dimension.INSTRUMENT_CODE]

    schema = pa.schema([
        pa.field(Dimension.INSTRUMENT_CODE, pa.string(),
                 metadata={b'table_field': b'BOND_CODE', b'table_name': b'INFO_FI_VAL_CNBD'}),
        pa.field(Dimension.TRADE_DATE, pa.string(),
                 metadata={b'table_field': b'TRD_DATE', b'table_name': b'INFO_FI_VAL_CNBD'}),
        pa.field(Dimension.REMN_TERM, pa.float64(),
                 metadata={b'table_field': b'RMN_YEAR', b'table_name': b'INFO_FI_VAL_CNBD'}),
        pa.field(Measure.YIELD, pa.float64(),
                 metadata={b'table_field': b'YIELD', b'table_name': b'INFO_FI_VAL_CNBD'}),
        pa.field(Measure.DURATION, pa.float64(),
                 metadata={b'table_field': b'MODIF_DUR', b'table_name': b'INFO_FI_VAL_CNBD'}),
        pa.field(Measure.VALUATION_NET_PRICE, pa.float64(),
                 metadata={b'table_field': b'NET_PRC', b'table_name': b'INFO_FI_VAL_CNBD'}),
        pa.field(Measure.VALUATION_FULL_PRICE, pa.float64(),
                 metadata={b'table_field': b'FULL_PRC', b'table_name': b'INFO_FI_VAL_CNBD'}),
        pa.field(Measure.INTEREST, pa.float64(),
                 metadata={b'table_field': b'PAY_INT', b'table_name': b'INFO_FI_CASHFLOW'}),
        pa.field(Measure.PRINCIPAL, pa.float64(),
                 metadata={b'table_field': b'PRCP_CASH', b'table_name': b'INFO_FI_CASHFLOW'}),
        pa.field(Measure.VAL_TYPE, pa.string(),
                 metadata={b'table_field': b'VAL_TYPE', b'table_name': b'INFO_FI_VAL_CNBD'}),
        pa.field(Measure.IS_RCM, pa.string(),
                 metadata={b'table_field': b'IS_RCM', b'table_name': b'INFO_FI_VAL_CNBD'}),
        pa.field(Measure.CASH_AMT_AFT, pa.float64(),
                 metadata={b'table_field': b'CASH_AMT_AFT', b'table_name': b'INFO_FI_CASHFLOW'}),
        pa.field(Measure.ACR_INT, pa.float64(),
                 metadata={b'table_field': b'ACR_INT', b'table_name': b'INFO_FI_VAL_CNBD'})
    ],
        metadata={
            Dimension.INSTRUMENT_CODE: '债券内部编码',
            Dimension.TRADE_DATE: '交易日期',
            Dimension.REMN_TERM: '剩余年限(年)',
            Measure.YIELD: '收益率（%）',
            Measure.DURATION: '修正久期',
            Measure.VALUATION_NET_PRICE: '净价（元）',
            Measure.VALUATION_FULL_PRICE: '全价（元）',
            Measure.INTEREST: '偿还利息(元)',
            Measure.PRINCIPAL: '偿付本金(元)',
            Measure.VAL_TYPE: '估值类型',
            Measure.IS_RCM: '是否推荐',
            Measure.CASH_AMT_AFT: '偿还后期末金额(元)',
            Measure.ACR_INT: '应计利息'

        }
    )

    @classmethod
    def fetch_bond_mkt_data(
            cls,
            secu_code: str = None,
            start_date: Optional[Union[datetime, date]] = None,
            end_date: Optional[Union[datetime, date]] = date.today()):
        sql = f"""select
                            BOND_CODE,
                            TRD_DATE,
                            RMN_YEAR,
                            YIELD,
                            MODIF_DUR,
                            NET_PRC,
                            FULL_PRC,
                            VAL_TYPE,
                            IS_RCM,
                            ACR_INT
                        FROM
                            INFO_FI_VAL_CNBD
                        where
                            BOND_CODE in  ({secu_code})
                            and YIELD is not null 
                            and DATA_SRC='001'
                            and TRD_DATE between '{start_date}' and '{end_date}'
                      
                    """
        df = cls.query(sql)
        if not df.empty:
            df = df.rename(columns=cls.etl_rename_dict)
            df[Dimension.TRADE_DATE] = df[Dimension.TRADE_DATE].apply(date_to_str)

        return df

    @classmethod
    def format_bond_mkt_data(cls, df):
        if not df.empty:
            df = df.rename(columns=cls.etl_rename_dict)
            df[Measure.PRINCIPAL] = df[Measure.PRINCIPAL].astype(float)
            df[Measure.INTEREST] = df[Measure.INTEREST].astype(float)
            df[[Measure.PRINCIPAL, Measure.INTEREST]] = df[[Measure.PRINCIPAL, Measure.INTEREST]].fillna(0)
            df[Measure.DURATION] = df[Measure.DURATION].astype(float)
            df[Dimension.REMN_TERM] = df[Dimension.REMN_TERM].astype(float)
            df[Measure.VALUATION_NET_PRICE] = df[Measure.VALUATION_NET_PRICE].astype(float)
            df[Measure.VALUATION_FULL_PRICE] = df[Measure.VALUATION_FULL_PRICE].astype(float)
            df[Measure.YIELD] = df[Measure.YIELD].astype(float) / 100
            df[Measure.VAL_TYPE] = df[Measure.VAL_TYPE].astype(str)
            df[Measure.IS_RCM] = df[Measure.IS_RCM].astype(str)
        return df

    etl_rename_dict = {
        'BOND_CODE': Dimension.INSTRUMENT_CODE,
        'TRD_DATE': Dimension.TRADE_DATE,
        'RMN_YEAR': Dimension.REMN_TERM,
        'YIELD': Measure.YIELD,
        'MODIF_DUR': Measure.DURATION,
        'NET_PRC': Measure.VALUATION_NET_PRICE,
        'FULL_PRC': Measure.VALUATION_FULL_PRICE,
        'PAY_INT': Measure.INTEREST,
        'PRCP_CASH': Measure.PRINCIPAL,
        'VAL_TYPE': Measure.VAL_TYPE,
        'IS_RCM': Measure.IS_RCM,
        'CASH_DATE': Dimension.TRADE_DATE,
        'CASH_AMT_AFT': Measure.CASH_AMT_AFT,
        'ACR_INT': Measure.ACR_INT
    }

    @classmethod
    def fetch_fi_cashflow(cls,
                   secu_code: str = None,
                   start_date: Optional[Union[datetime, date]] = None,
                   end_date: Optional[Union[datetime, date]] = None):
        sql = f"""
            select
                BOND_CODE,
                CASH_DATE,
                PAY_INT,
                PRCP_CASH,
                CASH_AMT_AFT
            from
                INFO_FI_CASHFLOW
            where
                IS_ACT_CF = 0
                and  BOND_CODE in  ({secu_code})
        """
        df = cls.query(sql)
        if not df.empty:
            df = df.rename(columns=cls.etl_rename_dict)
            df[Dimension.TRADE_DATE] = df[Dimension.TRADE_DATE].apply(date_to_str)

        return df


    @classmethod
    def fetch_data(cls,
                   secu_code: str = None,
                   start_date: Optional[Union[datetime, date]] = None,
                   end_date: Optional[Union[datetime, date]] = None):
        df_bond_mkt_data = cls.fetch_bond_mkt_data(secu_code, start_date, end_date)
        if df_bond_mkt_data.empty:
            return df_bond_mkt_data
        df_fi_cashflow_data = cls.fetch_fi_cashflow(secu_code, start_date, end_date)
        df = df_bond_mkt_data.merge(df_fi_cashflow_data, how='left')
        df = df.reset_index(drop=True)
        df = cls.format_bond_mkt_data(df)
        # 债券每日面值
        if len(df_fi_cashflow_data) and df[Measure.CASH_AMT_AFT].isnull().any():
            common_columns = [Dimension.INSTRUMENT_CODE, Dimension.TRADE_DATE]
            cashflow_columns = common_columns + [Measure.CASH_AMT_AFT]
            amt_aft_df = df_bond_mkt_data[common_columns].merge(df_fi_cashflow_data[cashflow_columns], how='outer')
            amt_aft_df = amt_aft_df.groupby(Dimension.INSTRUMENT_CODE, group_keys=False).apply(cls.fill_cash_amt_aft)
            df = df.drop(columns=[Measure.CASH_AMT_AFT])
            df = df.merge(amt_aft_df, how='left')
        df[Measure.CASH_AMT_AFT] = df[Measure.CASH_AMT_AFT].fillna(100)
        return df


    @classmethod
    def fill_cash_amt_aft(cls, g):
        if g[Measure.CASH_AMT_AFT].isnull().any():
            #  取INFO_FI_CASHFLOW.CASH_DATE<=当前日期 and IS_ACT_CF = 0最新一条的INFO_FI_CASHFLOW.CASH_AMT_AFT
            g[Measure.CASH_AMT_AFT] = g.sort_values(Dimension.TRADE_DATE, ascending=False)[Measure.CASH_AMT_AFT].bfill()
        return g

    @classmethod
    @resource_decorator(secu_type="BOND")
    def run_etl(cls, secu_codes: Optional[list[str]] = None,
                start_date: Optional[Union[datetime, date]] = None,
                end_date: Optional[Union[datetime, date]] = date.today(),
                is_concurrent_query=False,
                is_concurrent_save=False,
                is_init=False):
        """增加secu_code资源过滤"""
        return super(BondValCNBD, cls).run_etl(
            secu_codes=secu_codes,
            start_date=start_date,
            end_date=end_date,
            is_concurrent_query=is_concurrent_query,
            is_concurrent_save=is_concurrent_save,
            is_init=is_init)


if __name__ == '__main__':
    BondValCNBD.run_etl(start_date=date(2020, 1, 1))
