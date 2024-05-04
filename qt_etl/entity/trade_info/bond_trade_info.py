# vim set fileencoding=utf-8

from datetime import date, datetime
from typing import Optional, Union

import pyarrow as pa

from qt_common.utils import date_to_str
from qt_etl.entity.fields import Dimension, Measure
from qt_etl.entity.portfolio import resource_decorator
from qt_etl.entity.trade_info.trade_info import TradeInfo

__all__ = ["BondTradeInfo"]


class BondTradeInfo(TradeInfo):
    """债券交易数据"""
    main_table = 'INDIC_BASE_TX_BOND'
    schema = pa.schema([
        pa.field(Dimension.BOOK_ID, pa.string(), metadata={b"table_field": b"PRD_CODE"}),
        pa.field(Dimension.TRADE_DATE, pa.string(), metadata={b"table_field": b"TX_DATE"}),
        pa.field(Dimension.INSTRUMENT_CODE, pa.string(), metadata={b"table_field": b"SECU_CODE"}),
        pa.field(Measure.TRADE_NOMINAL, pa.float64(), metadata={b"table_field": b"TRAN_QTY"}),
        pa.field(Measure.TRADE_AMOUNT, pa.float64(), metadata={b"table_field": b"TRAN_NET_AMT"}),
        pa.field(Measure.SETTLE_AMOUNT, pa.float64(), metadata={b"table_field": b"ACTL_STL_AMT"}),
        pa.field(Measure.ACR_INT, pa.float64(), metadata={b"table_field": b"INT_AMT"})
    ],
        metadata={
            Dimension.BOOK_ID: "产品代码",
            Dimension.TRADE_DATE: '交易日期',
            Dimension.INSTRUMENT_CODE: '证券内码',
            Measure.TRADE_NOMINAL: '成交数量（张）',
            Measure.TRADE_AMOUNT: '成交金额（净价）',
            Measure.SETTLE_AMOUNT: '清算金额',
            Measure.ACR_INT: '应计利息'
        }
    )
    etl_rename_dict = {
        'PRD_CODE': Dimension.BOOK_ID,
        'TX_DATE': Dimension.TRADE_DATE,
        'SECU_CODE': Dimension.INSTRUMENT_CODE,
        'TRAN_QTY': Measure.TRADE_NOMINAL,
        'TRAN_NET_AMT': Measure.TRADE_AMOUNT,
        'ACTL_STL_AMT': Measure.SETTLE_AMOUNT,
        'INT_AMT': Measure.ACR_INT
    }
    buy_or_sell_map = {
        "T02.02.000.001": 1.0,
        "T02.02.000.002": -1.0,
    }
    measure_columns = [Measure.TRADE_NOMINAL, Measure.TRADE_AMOUNT, Measure.SETTLE_AMOUNT, Measure.ACR_INT]

    @classmethod
    def get_trade_data(cls,
                       secu_code: str = None,
                       start_date: Optional[Union[datetime, date]] = None,
                       end_date: Optional[Union[datetime, date]] = None):
        sql = f"""SELECT PRD_CODE,
                            TX_DATE,
                            SECU_CODE,
                            TX_TYPE_CODE,
                            TRAN_QTY,
                            TRAN_NET_AMT,
                            ACTL_STL_AMT,
                            INT_AMT
                            FROM INDIC_BASE_TX_BOND where TX_DATE between '{start_date}' and '{end_date}'
                """
        if secu_code:
            sql += f" AND SECU_CODE in ({secu_code})"
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
        df = cls.get_trade_data(secu_code=secu_code, start_date=start_date, end_date=end_date)
        df = cls.deal_data(df)
        return df

    @classmethod
    def deal_data(cls, df):
        if df.empty:
            return df

        df.TX_TYPE_CODE = df.TX_TYPE_CODE.map(cls.buy_or_sell_map)
        df[Measure.TRADE_NOMINAL] = df[Measure.TRADE_NOMINAL].astype(float) * df.TX_TYPE_CODE
        df[Measure.TRADE_AMOUNT] = df[Measure.TRADE_AMOUNT].astype(float) * df.TX_TYPE_CODE
        df[Measure.SETTLE_AMOUNT] = df[Measure.SETTLE_AMOUNT].astype(float) * df.TX_TYPE_CODE
        df = df.drop(columns='TX_TYPE_CODE')
        # groupby sum
        df = df.groupby([Dimension.BOOK_ID, Dimension.TRADE_DATE, Dimension.INSTRUMENT_CODE])[
            cls.measure_columns].sum().reset_index()
        return df

    @classmethod
    @resource_decorator(secu_type="BOND")
    def run_etl(cls, secu_codes: Optional[list[str]] = None,
                start_date: Optional[Union[datetime, date]] = None,
                end_date: Optional[Union[datetime, date]] = date.today(),
                is_concurrent_query=False,
                is_concurrent_save=False,
                is_init=False):
        super(BondTradeInfo, cls).run_etl(
            secu_codes=secu_codes,
            start_date=start_date,
            end_date=end_date,
            is_concurrent_save=is_concurrent_save,
            is_concurrent_query=is_concurrent_query,
            is_init=is_init)


if __name__ == '__main__':
    BondTradeInfo.run_etl(start_date=datetime(2021, 1, 1),
                          end_date=date(2021, 11, 10))
