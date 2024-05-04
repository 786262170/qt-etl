# vim set fileencoding=utf-8

from datetime import date, datetime
from typing import Optional, Union

import pandas as pd
import pyarrow as pa

from qt_common.qt_logging import frame_log
from qt_common.utils import date_to_str
from qt_etl.entity.fields import Dimension, Measure
from qt_etl.entity.trade_info.trade_info import TradeInfo

__all__ = ["StockTradeInfo"]


class StockTradeInfo(TradeInfo):
    """股票交易数据"""
    main_table = 'INDIC_BASE_TX_STOCK'
    # groupby columns
    info_columns = ['PRD_CODE', 'SECU_CODE', 'CLR_DATE']
    measure_columns = ['AMT_TRADE_FEE', 'SELL_AMT_TRADE_FEE', Measure.BUY_AMOUNT, Measure.SELL_AMOUNT, 'BUY_QTY','SELL_QTY']

    etl_rename_dict = {
        'PRD_CODE': Dimension.BOOK_ID,
        'CLR_DATE': Dimension.TRADE_DATE,
        'SECU_CODE': Dimension.INSTRUMENT_CODE,
        'CHG_TRADING_FEE': Measure.TRADING_FEE,
        # 成交数量
        'TRAN_QTY': Measure.TRADE_NOMINAL,
        'BUY_AMT_DAY': Measure.BUY_AMOUNT,
        'SELL_AMT_DAY': Measure.SELL_AMOUNT,
    }

    schema = pa.schema([
        pa.field(Dimension.BOOK_ID, pa.string(), metadata={b"table_field": b"PRD_CODE"}),
        pa.field(Dimension.TRADE_DATE, pa.string(), metadata={b"table_field": b"CLR_DATE"}),
        pa.field(Dimension.INSTRUMENT_CODE, pa.string(), metadata={b"table_field": b"SECU_CODE"}),
        pa.field(Measure.TRADING_FEE, pa.float64(), metadata={b"table_field": b'TX_FEE'}),
        pa.field(Measure.TRADE_NOMINAL, pa.float64(), metadata={b"table_field": b"TRAN_QTY"}),
        pa.field(Measure.BUY_AMOUNT, pa.float64(), metadata={b"table_field": b"TRAN_AMT"}),
        pa.field(Measure.SELL_AMOUNT, pa.float64(), metadata={b"table_field": b"TRAN_AMT"}),
    ],
        metadata={
            Dimension.BOOK_ID: "产品代码",
            Dimension.TRADE_DATE: '业务日期',
            Dimension.INSTRUMENT_CODE: '证券内码',
            Measure.TRADING_FEE: '应付交易费',
        }
    )

    @classmethod
    def get_buy_amt_day_data(cls):
        sql = """select
            PRD_CODE,
            CLR_DATE,
            SECU_CODE,
            TRAN_AMT AS BUY_AMT_DAY,
            TX_FEE AS AMT_TRADE_FEE,
            TRAN_QTY AS BUY_QTY
        FROM
            INDIC_BASE_TX_STOCK
        WHERE
            TX_TYPE_CODE = 'T01.01.000.001' 
          """
        df = cls.query(sql)
        return df

    @classmethod
    def get_buy_sell_day_data(cls):
        sql = """select
        PRD_CODE,
            CLR_DATE,
            SECU_CODE,
            TRAN_AMT AS SELL_AMT_DAY,
            TX_FEE AS SELL_AMT_TRADE_FEE,
            TRAN_QTY AS SELL_QTY
        FROM
            INDIC_BASE_TX_STOCK
        WHERE
            TX_TYPE_CODE = 'T01.01.000.002' 
          """
        df = cls.query(sql)
        return df

    @classmethod
    def get_stock_data(cls, secu_code=None, start_date=None, end_date=None):
        # 获取穿透前数据
        amt_df = cls.get_buy_amt_day_data()
        shell_df = cls.get_buy_sell_day_data()
        amt_df = amt_df.groupby(by=cls.info_columns).sum().reset_index()
        shell_df = shell_df.groupby(by=cls.info_columns).sum().reset_index()
        trd_stock_df = amt_df.merge(shell_df, how='outer')
        if trd_stock_df.empty:
            return pd.DataFrame()
        trd_stock_df = trd_stock_df.rename(columns=cls.etl_rename_dict)
        trd_stock_df[Dimension.TRADE_DATE] = trd_stock_df[Dimension.TRADE_DATE].apply(date_to_str)

        return trd_stock_df.reset_index(drop=True)

    @classmethod
    def deal_data(cls, all_df):
        all_df[cls.measure_columns] = all_df[cls.measure_columns].fillna(0)
        all_df['CHG_TRADING_FEE'] = all_df['AMT_TRADE_FEE'] + all_df['SELL_AMT_TRADE_FEE']
        all_df['TRAN_QTY'] = all_df['BUY_QTY'] - all_df['SELL_QTY']
        all_df.drop(columns=['BUY_QTY', 'SELL_QTY', 'AMT_TRADE_FEE', 'SELL_AMT_TRADE_FEE'], inplace=True)
        all_df = all_df.rename(columns=cls.etl_rename_dict)

        return all_df

    @classmethod
    def fetch_data(cls,
                   secu_code: str = None,
                   start_date: Optional[Union[datetime, date]] = None,
                   end_date: Optional[Union[datetime, date]] = None):
        trd_stock_df = cls.get_stock_data(secu_code=secu_code, start_date=start_date, end_date=end_date)
        if trd_stock_df.empty:
            frame_log.warning(f'{cls.__name__}穿透前数据为空')
            return trd_stock_df
        df = cls.deal_data(trd_stock_df)
        return df


if __name__ == '__main__':
    StockTradeInfo.run_etl()
