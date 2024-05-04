# vim set fileencoding=utf-8
"""INFO_FUND_EODPRICE"""

from datetime import datetime, date
from typing import Optional, Union

import pandas as pd
import pyarrow as pa
import json
from math import log

from qt_common.utils import date_to_str, str_to_date
from qt_etl.entity.fields import Dimension, Measure
from qt_etl.entity.market_data.market_data import MarketData
from qt_etl.entity.portfolio.comb_position import resource_decorator
from qt_etl.constants import PartitionByDateType
from qt_common.qt_logging import frame_log
from qt_etl.entity.market_data.qt_calendar import QtCalendar

__all__ = ["FundDailyQuote"]


class FundDailyQuote(MarketData):
    """基金市场数据"""
    partitioned_by_date = PartitionByDateType.month
    schema = pa.schema([
        pa.field(Dimension.INSTRUMENT_CODE, pa.string(), metadata={b'source_table': json.dumps(
            {'INFO_FUND_EODPRICE': 'FUND_CODE', 'INDIC_BASE_PORT_IDX': 'PRD_CODE'}).encode()}),
        pa.field(Dimension.TRADE_DATE, pa.string(), metadata={b'source_table': json.dumps(
            {'INFO_FUND_EODPRICE': 'END_DATE', 'INDIC_BASE_PORT_IDX': 'BIZ_DATE'}).encode()}),
        pa.field(Measure.VOLUME, pa.float64(),
                 metadata={b'source_table': json.dumps({'INFO_FUND_EODPRICE': 'VOL'}).encode()}),
        pa.field(Measure.TRADE_AMOUNT, pa.float64(),
                 metadata={b'source_table': json.dumps({'INFO_FUND_EODPRICE': 'AMT'}).encode()}),
        pa.field(Measure.HIGH, pa.float64(),
                 metadata={b'source_table': json.dumps({'INFO_FUND_EODPRICE': 'HI_PRC'}).encode()}),
        pa.field(Measure.LOW, pa.float64(),
                 metadata={b'source_table': json.dumps({'INFO_FUND_EODPRICE': 'LO_PRC'}).encode()}),
        pa.field(Measure.CLOSE, pa.float64(), metadata={b'source_table': json.dumps(
            {'INFO_FUND_EODPRICE': 'CLS_PRC', 'INDIC_BASE_PORT_IDX': 'UNIT_NAV'}).encode()}),
        pa.field(Measure.TURNOVER, pa.float64(),
                 metadata={b'source_table': json.dumps({'INFO_FUND_EODPRICE': 'TURN_RAT'}).encode()}),
        pa.field(Measure.RETURN_PERCENTAGE, pa.float64(), metadata={b'source_table': json.dumps(
            {'INFO_FUND_EODPRICE': 'PCT_CHG', 'INDIC_DERI_PORT_PRFT_IDX': 'DAY_UNIT_NAV_GROW_RATE'}).encode()}),
        pa.field(Measure.CLOSE_ADJUSTED, pa.float64(), metadata={b'source_table': json.dumps(
            {'INFO_FUND_EODPRICE': 'CLS_PRC_ADJ', 'INDIC_BASE_PORT_IDX': 'ADJ_UNIT_NAV'}).encode()}),
        pa.field(Measure.RETURN_PERCENTAGE_ADJUSTED, pa.float64(), metadata={}),
        pa.field(Measure.RETURN_PERCENTAGE_ADJUSTED_LOG, pa.float64(), metadata={}),
    ],
        metadata={
            Dimension.INSTRUMENT_CODE: '产品代码',
            Dimension.TRADE_DATE: '业务日期',
            Measure.VOLUME: '交易量',
            Measure.TRADE_AMOUNT: '交易数额',
            Measure.HIGH: '最高价',
            Measure.LOW: '最低价',
            Measure.CLOSE: '收盘价',
            Measure.TURNOVER: '周转率',
            Measure.RETURN_PERCENTAGE: '增长率',
            Measure.CLOSE_ADJUSTED: '复权收盘价',
            Measure.RETURN_PERCENTAGE_ADJUSTED: '复权增长率',
            Measure.RETURN_PERCENTAGE_ADJUSTED_LOG: '复权对数增长率',
        }
    )

    @classmethod
    def fetch_data(cls,
                   secu_code: str = None,
                   start_date: Optional[Union[datetime, date]] = None,
                   end_date: Optional[Union[datetime, date]] = None):
        df_fund = cls._get_fund_daily_quote(secu_code, start_date, end_date)
        df_portfolio = cls._get_portfolio_nav(secu_code, start_date, end_date)
        # 测试_get_custom_portfolio_nav打开
        # df_portfolio.loc[:5, 'close_adjusted'] = None

        # 获取ADJ_UNIT_NAV为空的
        empty_nav_mark = df_portfolio[Measure.CLOSE_ADJUSTED].isnull()
        empty_nav_df = df_portfolio[empty_nav_mark]
        if len(empty_nav_df):
            frame_log.info('ADJ_UNIT_NAV存在为空')
            start_date = empty_nav_df[Dimension.TRADE_DATE].min()
            end_date = empty_nav_df[Dimension.TRADE_DATE].max()
            # 转为date格式
            start_date = QtCalendar.get_prev_biz_date(str_to_date(start_date))
            end_date = str_to_date(end_date)

            instrument_codes = df_portfolio[Dimension.INSTRUMENT_CODE].unique().tolist()
            custom_nav_df = cls._get_custom_portfolio_nav(secu_code=instrument_codes, start_date=start_date, end_date=end_date)
            if custom_nav_df.empty:
                frame_log.warning(f'custom_nav获取为空，instrument_codes：{instrument_codes}, start_date:{start_date}, end_date:{end_date}')
            else:
                columns = [Dimension.INSTRUMENT_CODE, Dimension.TRADE_DATE]
                merge_df = empty_nav_df[columns].merge(custom_nav_df, how='right')
                merge_df = merge_df.fillna(empty_nav_df)
                df_portfolio = pd.concat([df_portfolio[~empty_nav_mark], merge_df]).reset_index(drop=True)

        df = pd.concat([df_fund, df_portfolio])

        if not df.empty:
            # t-1日复权单位净值
            df[Measure.PREV_CLOSE_ADJUSTED] = df.groupby([Dimension.INSTRUMENT_CODE])[Measure.CLOSE_ADJUSTED].shift(1)
            # t日收益率 =（t日复权单位净值 / t-1日复权单位净值）-1
            df[Measure.RETURN_PERCENTAGE_ADJUSTED] = df.apply(
                lambda row: row[Measure.CLOSE_ADJUSTED] / row[Measure.PREV_CLOSE_ADJUSTED] - 1 if (
                        row[Measure.CLOSE_ADJUSTED] and row[Measure.PREV_CLOSE_ADJUSTED]) else None, axis=1)
            # 都为null时，存文件会报错
            df[Measure.RETURN_PERCENTAGE_ADJUSTED] = df[Measure.RETURN_PERCENTAGE_ADJUSTED].astype(float)
            df[Measure.RETURN_PERCENTAGE_ADJUSTED_LOG] = df[Measure.RETURN_PERCENTAGE_ADJUSTED].apply(
                lambda x: log(x + 1) if x is not None else None)
            df[Measure.RETURN_PERCENTAGE_ADJUSTED_LOG] = df[Measure.RETURN_PERCENTAGE_ADJUSTED_LOG].astype(float)
            df = df.drop(columns=Measure.PREV_CLOSE_ADJUSTED)
        return df

    @classmethod
    def _get_fund_daily_quote(cls,
                              secu_code: str = None,
                              start_date: Optional[Union[datetime, date]] = None,
                              end_date: Optional[Union[datetime, date]] = None):
        sql = f"""select 
                            FUND_CODE,
                            END_DATE,
                            VOL,
                            AMT,
                            HI_PRC,
                            LO_PRC,
                            CLS_PRC,
                            CLS_PRC_ADJ,
                            TURN_RAT,
                            PCT_CHG
                        FROM INFO_FUND_EODPRICE
                        WHERE END_DATE between '{start_date}' and '{end_date}'"""
        if secu_code:
            sql += f" AND FUND_CODE in ({secu_code})"
        df = cls.query(sql)
        df = df.rename(columns={
            'FUND_CODE': Dimension.INSTRUMENT_CODE,
            'END_DATE': Dimension.TRADE_DATE,
            'VOL': Measure.VOLUME,
            'AMT': Measure.TRADE_AMOUNT,
            'HI_PRC': Measure.HIGH,
            'LO_PRC': Measure.LOW,
            'CLS_PRC': Measure.CLOSE,
            'CLS_PRC_ADJ': Measure.CLOSE_ADJUSTED,
            'TURN_RAT': Measure.TURNOVER,
            'PCT_CHG': Measure.RETURN_PERCENTAGE
        })
        df.sort_values(by=[Dimension.TRADE_DATE], inplace=True)
        df[Dimension.TRADE_DATE] = df[Dimension.TRADE_DATE].apply(date_to_str)
        df[Measure.RETURN_PERCENTAGE] = df[Measure.RETURN_PERCENTAGE].astype(float) / 100
        df = df.reset_index(drop=True)
        return df

    @classmethod
    def deal_nav(cls, df):
        """处理复权单位净值，复权单位净值日收益率"""
        if df.empty:
            return df
        
        df[Measure.PREV_QUANTITY] = df[Measure.QUANTITY].shift(1)
        # T日复权净值 = （T日市值-T日现金流）/T-1日份额
        df[Measure.CLOSE_ADJUSTED] = (df[Measure.CLOSE] - df[Measure.NET_CPTL_INFLOW_AMT]) / df[
            Measure.PREV_QUANTITY]
        # df[Measure.CLOSE_ADJUSTED] = df.apply(lambda row: (row[Measure.CLOSE] - row[Measure.NET_CPTL_INFLOW_AMT]) / row[
        #     Measure.PREV_QUANTITY], axis=1)
        # 复权单位净值日收益率
        df[Measure.PREV_CLOSE_ADJUSTED] = df[Measure.CLOSE_ADJUSTED].shift(1)
        df[Measure.RETURN_PERCENTAGE] = df[Measure.CLOSE_ADJUSTED] / df[Measure.PREV_CLOSE_ADJUSTED] - 1
        drop_columns = [Measure.PREV_CLOSE_ADJUSTED, Measure.PREV_QUANTITY, Measure.QUANTITY]
        df = df.drop(columns=drop_columns)
        return df
    

    @classmethod
    def _get_custom_portfolio_nav(cls,
                           secu_code: str = None,
                           start_date: Optional[Union[datetime, date]] = None,
                           end_date: Optional[Union[datetime, date]] = None):
        sql = f"""select
            PE_NET_ASSET,
            PE_SHARES,
            NET_CPTL_INFLOW_AMT,
            PRODUCT_CODE ,
            BUSI_DATE
        from
            DM_PORT_ASSET
            WHERE DOWN_FLAG='0'
        """
        cond = []
        if secu_code:
            # list转化为str
            secu_code_value = ','.join(["'%s'" %i  for i in secu_code])
            cond.append(f'PRODUCT_CODE IN ({secu_code_value})')
        if start_date and end_date:
            cond.append(f"BUSI_DATE BETWEEN '{start_date}' and '{end_date}' ")
        if cond:
            where_conditon = ' AND '.join(cond)
            sql = f'{sql} WHERE {where_conditon}'
        # PE_NET_ASSET, 净资产_期末
        # PE_SHARES, 份额_期末
        # NET_CPTL_INFLOW_AMT 组合净资金流入金额
        df = cls.query(sql)
        df = df.rename(columns={
            'PRODUCT_CODE': Dimension.INSTRUMENT_CODE,
            'BUSI_DATE': Dimension.TRADE_DATE,
            'PE_NET_ASSET': Measure.CLOSE,
            'PE_SHARES': Measure.QUANTITY,
            'NET_CPTL_INFLOW_AMT': Measure.NET_CPTL_INFLOW_AMT,
        })

        if not df.empty:
            start_date_res_df = pd.DataFrame()
            df[Dimension.TRADE_DATE] = df[Dimension.TRADE_DATE].apply(date_to_str).astype(str)
            not_start_date_df = df
            # 判断是否有起始日
            product_start_date_df = cls.get_product_start_date(secu_code=secu_code)
            if not product_start_date_df.empty:
                merged = df.merge(product_start_date_df, how='left', indicator=True)
                # 交集（起始日df）
                start_date_df = merged[merged['_merge'] == 'both']
                if not start_date_df.empty:
                    frame_log.info('包含产品起始日日')
                    # T日复权单位净值=1 T日份额=T日净资产 T日复权单位净值收益为空
                    start_date_res_df = start_date_df.copy()
                    start_date_res_df[Measure.CLOSE_ADJUSTED] = 1
                    start_date_res_df[Measure.QUANTITY] = start_date_df[Measure.CLOSE]
                    start_date_res_df[Measure.RETURN_PERCENTAGE] = None
                    drop_columns = [Measure.QUANTITY, '_merge']
                    start_date_res_df = start_date_res_df.drop(columns=drop_columns)
                # 非起始日df
                not_start_date_df = merged[merged['_merge'] == 'left_only']

            not_start_date_res = cls.deal_nav(df)
            if start_date_res_df.empty:
                res_df = not_start_date_res
            else:
                not_start_date_res = not_start_date_res.loc[not_start_date_df.index]
                res_df = pd.concat([not_start_date_res, start_date_res_df]).reset_index(drop=True)
        else:
            res_df = pd.DataFrame()
        return res_df


    @classmethod
    def get_product_start_date(cls, secu_code=None):
        """获取产品起始日"""
        sql = """SELECT
            PRODUCT_CODE ,
            SET_UP_DATE
        FROM
            DM_PORT_ASSET
        """

        group_sql = " group by PRODUCT_CODE, SET_UP_DATE"
        cond = []
        if secu_code:
            # list转化为str
            secu_code_value = ','.join(["'%s'" % i for i in secu_code])
            cond.append(f'PRODUCT_CODE IN ({secu_code_value})')
        if cond:
            where_conditon = ' AND '.join(cond)
            sql = f'{sql} WHERE {where_conditon}'
        sql = sql + group_sql
        df = cls.query(sql)
        if not df.empty:
            df = df.rename(columns={
                'PRD_CODE': Dimension.INSTRUMENT_CODE,
                'BIZ_DATE': Dimension.TRADE_DATE,
            })
            df[Dimension.TRADE_DATE] = df[Dimension.TRADE_DATE].apply(date_to_str)
        return df

   


    @classmethod
    def _get_portfolio_nav(cls,
                           secu_code: str = None,
                           start_date: Optional[Union[datetime, date]] = None,
                           end_date: Optional[Union[datetime, date]] = None):
        sql = f"""SELECT
            idx.PRD_CODE ,
            idx.BIZ_DATE,
            idx.UNIT_NAV,
            idx.ADJ_UNIT_NAV,
            prft_idx.DAY_UNIT_NAV_GROW_RATE
        FROM
            INDIC_BASE_PORT_IDX idx
        inner join INDIC_DERI_PORT_PRFT_IDX prft_idx
        
        on
            idx.BIZ_DATE = prft_idx.BIZ_DATE
            AND idx.PRD_CODE = prft_idx.PRD_CODE
        WHERE idx.BIZ_DATE between '{start_date}' and '{end_date}'
    """
        # if secu_code:
        #     sql += f" AND PRD_CODE in ({secu_code})"
        df = cls.query(sql)
        df = df.rename(columns={
            'PRD_CODE': Dimension.INSTRUMENT_CODE,
            'BIZ_DATE': Dimension.TRADE_DATE,
            'UNIT_NAV': Measure.CLOSE,
            'DAY_UNIT_NAV_GROW_RATE': Measure.RETURN_PERCENTAGE,
            'ADJ_UNIT_NAV': Measure.CLOSE_ADJUSTED,
        })

        if not df.empty:
            df[Measure.CLOSE] = df[Measure.CLOSE].astype(float)
            df[Measure.RETURN_PERCENTAGE] = df[Measure.RETURN_PERCENTAGE].astype(float)
            df[Measure.CLOSE_ADJUSTED] = df[Measure.CLOSE_ADJUSTED].astype(float)
            df[Dimension.TRADE_DATE] = df[Dimension.TRADE_DATE].apply(date_to_str).astype(str)

        return df

    @classmethod
    @resource_decorator(secu_type="FUND")
    def run_etl(cls, secu_codes: Optional[list[str]] = None,
                start_date: Optional[Union[datetime, date]] = None,
                end_date: Optional[Union[datetime, date]] = date.today(),
                is_concurrent_query=False,
                is_concurrent_save=False,
                is_init=False):
        super(FundDailyQuote, cls).run_etl(
            secu_codes=secu_codes,
            start_date=start_date,
            end_date=end_date,
            is_concurrent_save=is_concurrent_save,
            is_concurrent_query=is_concurrent_query,
            is_init=is_init)


if __name__ == '__main__':
    FundDailyQuote.run_etl(start_date=date(2021, 1, 20), end_date=date(2022, 2, 20))