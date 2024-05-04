# vim set fileencoding=utf-8
"""
数据融合
balance_sheet
cashflow
derived
income
"""
import asyncio
import json
from datetime import date, datetime
from typing import Optional, Union

import pyarrow as pa

from qt_common import async_helper
from qt_common.utils import date_to_str
from qt_etl.constants import PartitionByDateType
from qt_etl.entity.factor.factor import Factor
from qt_etl.entity.fields import Dimension, Fundamental

__all__ = ['FinancialIndicator']


class FinancialIndicator(Factor):
    """财务指标"""
    partitioned_by_date = PartitionByDateType.month
    # pa.schema字段的顺序要和df的顺序一致
    # PARTY_CODE
    party_code_souce_table = {
        "INFO_PARTY_FIN_BALANCE": "PARTY_CODE",
        "INFO_PARTY_FIN_INCOME": "PARTY_CODE",
        "INFO_PARTY_FIN_INDDRV": "PARTY_CODE",
        "INFO_PARTY_FIN_CASHFLOW": "PARTY_CODE",
    }

    # RPT_TYPE
    rpt_type_souce_table = {
        "INFO_PARTY_FIN_BALANCE": "RPT_TYPE",
        "INFO_PARTY_FIN_INCOME": "RPT_TYPE",
        "INFO_PARTY_FIN_INDDRV": "RPT_TYPE",
        "INFO_PARTY_FIN_CASHFLOW": "RPT_TYPE",
    }

    # RPT_TERM_TYPE
    rpt_term_type_souce_table = {
        "INFO_PARTY_FIN_BALANCE": "RPT_TERM_TYPE",
        "INFO_PARTY_FIN_INCOME": "RPT_TERM_TYPE",
        "INFO_PARTY_FIN_INDDRV": "RPT_TERM_TYPE",
        "INFO_PARTY_FIN_CASHFLOW": "RPT_TERM_TYPE",
    }

    # ANC_DATE
    anc_date_souce_table = {
        "INFO_PARTY_FIN_BALANCE": "ANC_DATE",
        "INFO_PARTY_FIN_INCOME": "ANC_DATE",
        "INFO_PARTY_FIN_INDDRV": "ANC_DATE",
        "INFO_PARTY_FIN_CASHFLOW": "ANC_DATE",
    }

    # END_DATE
    end_date_souce_table = {
        "INFO_PARTY_FIN_BALANCE": "END_DATE",
        "INFO_PARTY_FIN_INCOME": "END_DATE",
        "INFO_PARTY_FIN_INDDRV": "END_DATE",
        "INFO_PARTY_FIN_CASHFLOW": "END_DATE",
    }
    schema = pa.schema([
        pa.field(Dimension.ISSUER_CODE, pa.string(),
                 metadata={b"source_table": json.dumps(party_code_souce_table).encode()}),
        pa.field(Dimension.REPORT_TYPE, pa.string(),
                 metadata={b'source_table': json.dumps(rpt_type_souce_table).encode()}),
        pa.field(Dimension.REPORTING_PERIOD_TYPE, pa.string(),
                 metadata={b'source_table': json.dumps(rpt_term_type_souce_table).encode()}),
        pa.field(Dimension.TRADE_DATE, pa.string(),
                 metadata={b'source_table': json.dumps(anc_date_souce_table).encode()}),
        pa.field(Dimension.DUE_DATE, pa.string(),
                 metadata={b'source_table': json.dumps(end_date_souce_table).encode()}),
        pa.field(Fundamental.TOTAL_NON_CURRENT_LIABILITIES, pa.float64(),
                 metadata={b'table_field': b'LIAB_NONCUR', b"table_name": b"INFO_PARTY_FIN_BALANCE"}),
        pa.field(Fundamental.TOTAL_LIABILITIES, pa.float64(),
                 metadata={b'table_field': b'LIAB_TTL', b"table_name": b"INFO_PARTY_FIN_BALANCE"}),
        pa.field(Fundamental.TOTAL_ASSETS, pa.float64(),
                 metadata={b'table_field': b'AST_TTL', b"table_name": b"INFO_PARTY_FIN_BALANCE"}),
        pa.field(Fundamental.MONETARY_FUND, pa.float64(),
                 metadata={b'table_field': b'MNY_FUND', b"table_name": b"INFO_PARTY_FIN_BALANCE"}),
        pa.field(Fundamental.HELD_FOR_TRADING_FINANCIAL_ASSETS, pa.float64(),
                 metadata={b'table_field': b'FIN_AST_TRAS', b"table_name": b"INFO_PARTY_FIN_BALANCE"}),
        pa.field(Fundamental.NOTES_RECEIVABLE, pa.float64(),
                 metadata={b'table_field': b'RCV_NOTE', b"table_name": b"INFO_PARTY_FIN_BALANCE"}),
        pa.field(Fundamental.TOTAL_INVESTORS_EQUITY, pa.float64(),
                 metadata={b'table_field': b'OE_TTL', b"table_name": b"INFO_PARTY_FIN_BALANCE"}),
        pa.field(Fundamental.MINORITY_INTEREST, pa.float64(),
                 metadata={b'table_field': b'OE_MINOR', b"table_name": b"INFO_PARTY_FIN_BALANCE"}),
        pa.field(Fundamental.PERATING_INCOME, pa.float64(),
                 metadata={b'table_field': b'INC_BIZ', b"table_name": b"INFO_PARTY_FIN_INCOME"}),
        pa.field(Fundamental.NET_RETURN, pa.float64(),
                 metadata={b'table_field': b'NET_PFT', b"table_name": b"INFO_PARTY_FIN_INCOME"}),
        pa.field(Fundamental.TOTAL_PROFIT, pa.float64(),
                 metadata={b'table_field': b'TTL_PFT', b"table_name": b"INFO_PARTY_FIN_INCOME"}),
        pa.field(Fundamental.CASH_PAID_FOR_LONG_TERM_ASSETS, pa.float64(),
                 metadata={b'table_field': b'PAY_OBT_FIOLTA', b"table_name": b"INFO_PARTY_FIN_CASHFLOW"}),
        pa.field(Fundamental.NET_CASH_RECOVERED, pa.float64(),
                 metadata={b'table_field': b'RECP_DISP_FIOLTA', b"table_name": b"INFO_PARTY_FIN_CASHFLOW"}),
        pa.field(Fundamental.INVESTMENT_ACTIVITIES_CASH_FLOW, pa.float64(),
                 metadata={b'table_field': b'ICF', b"table_name": b"INFO_PARTY_FIN_CASHFLOW"}),
        pa.field(Fundamental.OCF, pa.float64(),
                 metadata={b'table_field': b'OCF', b"table_name": b"INFO_PARTY_FIN_CASHFLOW"}),
        pa.field(Fundamental.NET_INCREASE_IN_EQUIVALENTS, pa.float64(),
                 metadata={b'table_field': b'ICR_CASH_EQLT', b"table_name": b"INFO_PARTY_FIN_CASHFLOW"}),
        pa.field(Fundamental.EQUIVALENTS_BALANCE, pa.float64(),
                 metadata={b'table_field': b'CASH_EQLT_EOP', b"table_name": b"INFO_PARTY_FIN_CASHFLOW"}),
        pa.field(Fundamental.OPERATING_PROFIT, pa.float64(),
                 metadata={b'table_field': b'EPS_DIL_OP', b"table_name": b"INFO_PARTY_FIN_INDDRV"}),
        pa.field(Fundamental.RETURN_ON_TOTAL_ASSETS_FLOAT, pa.float64(),
                 metadata={b'table_field': b'ROA', b"table_name": b"INFO_PARTY_FIN_INDDRV"}),
        pa.field(Fundamental.GROSS_PROFIT_MARGIN_FLOAT, pa.float64(),
                 metadata={b'table_field': b'GRO_PRO_MAR', b"table_name": b"INFO_PARTY_FIN_INDDRV"}),
        pa.field(Fundamental.TOTAL_OPERATING_INCOME, pa.float64(),
                 metadata={b'table_field': b'GRO_MAR', b"table_name": b"INFO_PARTY_FIN_INDDRV"}),
        pa.field(Fundamental.TOTAL_ASSETS_TURNOVER, pa.float64(),
                 metadata={b'table_field': b'ASS_RN', b"table_name": b"INFO_PARTY_FIN_INDDRV"}),
        pa.field(Fundamental.AMORTIZATION_AND_ACCRUAL, pa.float64(),
                 metadata={b'table_field': b'STM_IS', b"table_name": b"INFO_PARTY_FIN_INDDRV"}),
        pa.field(Fundamental.INTEREST_FREE_CURRENT_LIABILITIES, pa.float64(),
                 metadata={b'table_field': b'EXI_CUR', b"table_name": b"INFO_PARTY_FIN_INDDRV"}),
        pa.field(Fundamental.INTEREST_FREE_NON_CURRENT_LIABILITIES, pa.float64(),
                 metadata={b'table_field': b'EXI_NON', b"table_name": b"INFO_PARTY_FIN_INDDRV"}),
        pa.field(Fundamental.EARNINGS_PER_SHARE, pa.float64(),
                 metadata={b'table_field': b'ORPS', b"table_name": b"INFO_PARTY_FIN_INDDRV"}),
        pa.field(Fundamental.EBIT, pa.float64(),
                 metadata={b'table_field': b'EBIT', b"table_name": b"INFO_PARTY_FIN_INDDRV"}),
    ],
        metadata={
            Dimension.ISSUER_CODE: '主体内部编码',
            Dimension.REPORT_TYPE: '报表类型',
            Dimension.REPORTING_PERIOD_TYPE: '报告期类型',
            Dimension.TRADE_DATE: '信息发布日期',
            Dimension.DUE_DATE: '截止日期',
            Fundamental.TOTAL_NON_CURRENT_LIABILITIES: '非流动负债合计',
            Fundamental.TOTAL_LIABILITIES: '负债合计',
            Fundamental.TOTAL_ASSETS: '资产总计',
            Fundamental.MONETARY_FUND: '货币资金',
            Fundamental.HELD_FOR_TRADING_FINANCIAL_ASSETS: '交易性金融资产',
            Fundamental.NOTES_RECEIVABLE: '应收票据',
            Fundamental.TOTAL_INVESTORS_EQUITY: '所有者权益（或股东权益）合计',
            Fundamental.MINORITY_INTEREST: '少数股东权益',
            Fundamental.PERATING_INCOME: '字段来源-营业收入',
            Fundamental.NET_RETURN: '字段来源-净利润',
            Fundamental.EBIT: '息税前利润(元)',
            Fundamental.TOTAL_PROFIT: '字段来源-利润总额',
            Fundamental.OPERATING_PROFIT: '摊薄每股收益(营业利润)(元)',
            Fundamental.RETURN_ON_TOTAL_ASSETS_FLOAT: '总资产净利率_平均(含少数股东损益的净利润)(%)',
            Fundamental.GROSS_PROFIT_MARGIN_FLOAT: '销售毛利率(%)',
            Fundamental.TOTAL_OPERATING_INCOME: '营业总收入(元)',
            Fundamental.TOTAL_ASSETS_TURNOVER: '总资产周转率(次)',
            Fundamental.AMORTIZATION_AND_ACCRUAL: '当期计提折旧与摊销(元)',
            Fundamental.INTEREST_FREE_CURRENT_LIABILITIES: '无息流动负债(元)',
            Fundamental.INTEREST_FREE_NON_CURRENT_LIABILITIES: '无息非流动负债(元)',
            Fundamental.EARNINGS_PER_SHARE: '每股营业收入(元)',
            Fundamental.CASH_PAID_FOR_LONG_TERM_ASSETS: '购建固定资产、无形资产和其他长期资产支付的现金',
            Fundamental.NET_CASH_RECOVERED: '处置固定资产、无形资产和其他长期资产所收回的现金净额',
            Fundamental.INVESTMENT_ACTIVITIES_CASH_FLOW: '投资活动产生的现金流量净额',
            Fundamental.OCF: '经营活动产生的现金流量净额',
            Fundamental.NET_INCREASE_IN_EQUIVALENTS: '现金及现金等价物净增加额',
            Fundamental.EQUIVALENTS_BALANCE: '期末现金及现金等价物余额',
        }
    )

    @classmethod
    def fetch_data(cls,
                   secu_code: str = None,
                   start_date: Optional[Union[datetime, date]] = None,
                   end_date: Optional[Union[datetime, date]] = None):
        df1 = cls.etl_balane_sheet_market_data(start_date, end_date)
        df2 = cls.etl_cash_flow_statement_market_data(start_date, end_date)
        df3 = cls.etl_income_statement_market_data(start_date, end_date)
        df4 = cls.etl_derived_finacial_stat_market_data(start_date, end_date)

        df = df1.merge(df2, how='outer',
                       on=['PARTY_CODE', 'RPT_TYPE', 'RPT_TERM_TYPE', 'ANC_DATE', 'END_DATE'])
        df = df.merge(df3, how='outer',
                      on=['PARTY_CODE', 'RPT_TYPE', 'RPT_TERM_TYPE', 'ANC_DATE', 'END_DATE'])
        df = df.merge(df4, how='outer',
                      on=['PARTY_CODE', 'RPT_TYPE', 'RPT_TERM_TYPE', 'ANC_DATE', 'END_DATE'])
        df = df.astype(cls.type_dict)
        df = df.rename(columns=cls.etl_rename_dict)
        # todo OCF为空
        df[Dimension.TRADE_DATE] = df[Dimension.TRADE_DATE].apply(date_to_str)
        df[Dimension.DUE_DATE] = df[Dimension.DUE_DATE].apply(date_to_str)
        df.set_index([Dimension.ISSUER_CODE, Dimension.TRADE_DATE])
        df = df.sort_values(by=[Dimension.TRADE_DATE, Dimension.ISSUER_CODE], ascending=False)
        df[Fundamental.RETURN_ON_TOTAL_ASSETS_FLOAT] = df[Fundamental.RETURN_ON_TOTAL_ASSETS_FLOAT].astype(
            float) / 100
        df[Fundamental.GROSS_PROFIT_MARGIN_FLOAT] = df[Fundamental.GROSS_PROFIT_MARGIN_FLOAT].astype(
            float) / 100
        df = df.reset_index(drop=True)
        return df

    etl_rename_dict = {
        'PARTY_CODE': Dimension.ISSUER_CODE,
        'RPT_TYPE': Dimension.REPORT_TYPE,
        'RPT_TERM_TYPE': Dimension.REPORTING_PERIOD_TYPE,
        'ANC_DATE': Dimension.TRADE_DATE,
        'END_DATE': Dimension.DUE_DATE,

        'LIAB_NONCUR': Fundamental.TOTAL_NON_CURRENT_LIABILITIES,
        'LIAB_TTL': Fundamental.TOTAL_LIABILITIES,
        'AST_TTL': Fundamental.TOTAL_ASSETS,
        'MNY_FUND': Fundamental.MONETARY_FUND,
        'FIN_AST_TRAS': Fundamental.HELD_FOR_TRADING_FINANCIAL_ASSETS,
        'RCV_NOTE': Fundamental.NOTES_RECEIVABLE,
        'OE_TTL': Fundamental.TOTAL_INVESTORS_EQUITY,
        'OE_MINOR': Fundamental.MINORITY_INTEREST,

        'INC_BIZ': Fundamental.PERATING_INCOME,
        'NET_PFT': Fundamental.NET_RETURN,
        'EBIT': Fundamental.EBIT,
        'TTL_PFT': Fundamental.TOTAL_PROFIT,

        'EPS_DIL_OP': Fundamental.OPERATING_PROFIT,
        'ROA': Fundamental.RETURN_ON_TOTAL_ASSETS_FLOAT,
        'GRO_PRO_MAR': Fundamental.GROSS_PROFIT_MARGIN_FLOAT,
        'GRO_MAR': Fundamental.TOTAL_OPERATING_INCOME,
        'ASS_RN': Fundamental.TOTAL_ASSETS_TURNOVER,
        'STM_IS': Fundamental.AMORTIZATION_AND_ACCRUAL,
        'EXI_CUR': Fundamental.INTEREST_FREE_CURRENT_LIABILITIES,
        'EXI_NON': Fundamental.INTEREST_FREE_NON_CURRENT_LIABILITIES,
        'ORPS': Fundamental.EARNINGS_PER_SHARE,

        'PAY_OBT_FIOLTA': Fundamental.CASH_PAID_FOR_LONG_TERM_ASSETS,
        'RECP_DISP_FIOLTA': Fundamental.NET_CASH_RECOVERED,
        'ICF': Fundamental.INVESTMENT_ACTIVITIES_CASH_FLOW,
        'OCF': Fundamental.OCF,
        'ICR_CASH_EQLT': Fundamental.NET_INCREASE_IN_EQUIVALENTS,
        'CASH_EQLT_EOP': Fundamental.EQUIVALENTS_BALANCE,
    }

    type_dict = {
        'LIAB_NONCUR': float,
        'LIAB_TTL': float,
        'AST_TTL': float,
        'MNY_FUND': float,
        'FIN_AST_TRAS': float,
        'RCV_NOTE': float,
        'OE_TTL': float,
        'OE_MINOR': float,

        'INC_BIZ': float,
        'NET_PFT': float,
        'EBIT': float,
        'TTL_PFT': float,

        'EPS_DIL_OP': float,
        'ROA': float,
        'GRO_PRO_MAR': float,
        'GRO_MAR': float,
        'ASS_RN': float,
        'STM_IS': float,
        'EXI_CUR': float,
        'EXI_NON': float,
        'ORPS': float,

        'PAY_OBT_FIOLTA': float,
        'RECP_DISP_FIOLTA': float,
        'ICF': float,
        'OCF': float,
        'ICR_CASH_EQLT': float,
        'CASH_EQLT_EOP': float,
    }

    @classmethod
    def etl_balane_sheet_market_data(cls, start_date, end_date):
        sql = f"""SELECT 
                 PARTY_CODE,
                 RPT_TYPE,
                 RPT_TERM_TYPE,
                 ANC_DATE,
                 END_DATE,
                 LIAB_NONCUR,
                 LIAB_TTL,
                 AST_TTL,
                 MNY_FUND,
                 FIN_AST_TRAS,
                 RCV_NOTE,
                 OE_TTL,    
                 OE_MINOR
             FROM
                 INFO_PARTY_FIN_BALANCE
             WHERE RPT_TYPE = '003'
             AND ANC_DATE between '{start_date}' and '{end_date}'"""
        df = cls.query(sql)
        return df

    @classmethod
    def etl_cash_flow_statement_market_data(cls, start_date, end_date):
        sql = f"""SELECT
            PARTY_CODE,
            RPT_TYPE,
            RPT_TERM_TYPE,
            ANC_DATE,
            END_DATE,
            INC_BIZ,
            NET_PFT,
            TTL_PFT
        FROM
            INFO_PARTY_FIN_INCOME
         WHERE RPT_TERM_TYPE = '004'
         AND RPT_TYPE = '003'
         AND ANC_DATE between '{start_date}' and '{end_date}'"""
        df = cls.query(sql)
        return df

    @classmethod
    def etl_derived_finacial_stat_market_data(cls, start_date, end_date):
        sql = f"""
            SELECT
                PARTY_CODE,
                RPT_TYPE,
                RPT_TERM_TYPE,
                ANC_DATE_LAT,
                END_DATE,
                EPS_DIL_OP,
                ROA,
                GRO_PRO_MAR,
                GRO_MAR,
                ASS_RN,
                STM_IS,
                EXI_CUR,
                EXI_NON,
                ORPS,
                EBIT
            FROM
                INFO_PARTY_FIN_INDDRV
            WHERE ANC_DATE_LAT between '{start_date}' and '{end_date}'"""

        df = cls.query(sql)
        df = df.rename(columns={'ANC_DATE_LAT': 'ANC_DATE'})
        return df

    @classmethod
    def etl_income_statement_market_data(cls, start_date, end_date):
        sql = f"""SELECT
                RPT_TYPE,
                RPT_TERM_TYPE,
                PARTY_CODE,
                ANC_DATE,
                END_DATE,
                PAY_OBT_FIOLTA,
                RECP_DISP_FIOLTA,
                ICF,
                OCF,
                ICR_CASH_EQLT,
                CASH_EQLT_EOP
            FROM
                INFO_PARTY_FIN_CASHFLOW
            WHERE RPT_TERM_TYPE = '004'
            AND RPT_TYPE = '003'
            AND ANC_DATE between '{start_date}' and '{end_date}'"""
        df = cls.query(sql)
        return df


if __name__ == '__main__':
    FinancialIndicator.run_etl(start_date=date(2022, 1, 5), end_date=date(2022, 3, 3))
