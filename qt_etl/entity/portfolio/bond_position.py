# vim set fileencoding=utf-8
"""组合持仓 etl save"""
from datetime import date, datetime
from typing import Optional, Union

import pandas as pd
import pyarrow as pa

from qt_common.qt_logging import frame_log
from qt_common.utils import date_to_str
from qt_etl.entity.fields import Dimension, Measure
from qt_etl.entity.portfolio.portfolio import Portfolio

__all__ = ["BondPosition"]


class BondPosition(Portfolio):
    """债券持仓明细表"""
    main_table = 'INDIC_BASE_PORT_POS_DTL'
    schema = pa.schema([
        pa.field(Dimension.BOOK_ID, pa.string(), metadata={b"table_field": b"PRD_CODE"}),
        pa.field(Dimension.CURRENCY, pa.string(), metadata={b"table_field": b"CUR_CODE"}),
        pa.field(Dimension.TRADE_DATE, pa.string(), metadata={b"table_field": b"BIZ_DATE"}),
        pa.field(Dimension.INSTRUMENT_CODE, pa.string(), metadata={b"table_field": b"SECU_CODE"}),
        pa.field(Dimension.SECU_TYPE_CODE, pa.string(), metadata={b"table_field": b"SECU_TYPE_CODE"}),
        pa.field(Dimension.SYMBOL, pa.string(), metadata={b"table_field": b"SYMBOL"}),
        pa.field(Dimension.INSTRUMENT_NAME, pa.string(), metadata={b"table_field": b"SECU_NAME"}),
        pa.field(Measure.UNIT_COST, pa.float64(), metadata={b"table_field": b"UNIT_COST"}),
        pa.field(Measure.VALUATION_PRICE, pa.float64(), metadata={b"table_field": b"VAL_PRC"}),
        pa.field(Measure.COST, pa.float64(), metadata={b"table_field": b"POS_COST"}),
        pa.field(Measure.MARKET_VALUE, pa.float64(), metadata={b"table_field": b"POS_MKV"}),
        pa.field(Measure.QUANTITY, pa.float64(), metadata={b"table_field": b"POS_QTY"}),
        # shift
        # pa.field(Measure.PREV_QUANTITY, pa.float64(), metadata={b"table_field": b""}),
        # pa.field(Measure.PREV_MARKET_VALUE, pa.float64(), metadata={b"table_field": b""}),
        # pa.field(Measure.PREV_COST, pa.float64(), metadata={b"table_field": b""}),
        pa.field(Dimension.HELD_TO_MATURITY, pa.bool_(), metadata={b"table_field": b"INVES_CLS_CODE"}),
        pa.field(Dimension.TRADING_MARKET, pa.string(), metadata={b"table_field": b"TX_MKT_CODE"}),
        # pa.field(Dimension.REMN_TERM, pa.float64(),
        #          metadata={b"table_field": b'REMN_TERM', b'table_name': b'INDIC_BASE_REPO_POS_DTL'}),
        pa.field(Measure.RMN_COST, pa.float64(), metadata={b"table_field": b"RMN_COST"}),
        pa.field(Measure.SHADOW_MARKET_VALUE, pa.float64(), metadata={b"table_field": b"SHADOW_MKV"}),
    ])
    etl_rename_dict = {
        'PRD_CODE': Dimension.BOOK_ID,
        'CUR_CODE': Dimension.CURRENCY,
        'BIZ_DATE': Dimension.TRADE_DATE,
        'SECU_CODE': Dimension.INSTRUMENT_CODE,
        'SECU_NAME': Dimension.INSTRUMENT_NAME,
        'SYMBOL': Dimension.SYMBOL,
        'SECU_TYPE_CODE': Dimension.SECU_TYPE_CODE,
        'POS_MKV': Measure.MARKET_VALUE,
        'POS_QTY': Measure.QUANTITY,
        'UNIT_COST': Measure.UNIT_COST,
        'POS_COST': Measure.COST,
        'VAL_PRC': Measure.VALUATION_PRICE,
        'INVES_CLS_CODE': Dimension.HELD_TO_MATURITY,
        'TX_MKT_CODE': Dimension.TRADING_MARKET,
        'RMN_COST': Measure.RMN_COST,
        'SHADOW_MKV': Measure.SHADOW_MARKET_VALUE
    }

    @classmethod
    def get_pos_dtl_data(cls,
                         secu_code: str = None,
                         start_date: Optional[Union[datetime, date]] = date(2020, 1, 1),
                         end_date: Optional[Union[datetime, date]] = None):
        """
        债券持仓明细表
        :param secu_code:
        :param start_date:
        :param end_date:
        :return:
        """
        # LAST_DAY_POS_QTY, LAST_DAY_POS_MKV, LAST_DAY_POS_COST不从表里读取
        sql = f"""select
      
                         PRD_CODE,
                         CUR_CODE,
                         BIZ_DATE,
                         SECU_CODE,
                         SECU_TYPE_CODE,
                         SYMBOL,
                         SECU_NAME,
                         max(UNIT_COST) as UNIT_COST,
                         max(VAL_PRC) as VAL_PRC,
                         case when sum(POS_QTY) =0 then 0 else  sum(POS_QTY * POS_COST)/ sum(POS_QTY) end as POS_COST ,
                         sum(POS_MKV) as POS_MKV, sum(POS_QTY) as POS_QTY,
                         INVES_CLS_CODE, TX_MKT_CODE , RMN_COST, SHADOW_MKV
                     FROM
                         INDIC_BASE_BOND_POS_DTL
                     WHERE
                         BIZ_DATE between '{start_date}' and '{end_date}'
                     group by
                         PRD_CODE,
                         CUR_CODE,
                         BIZ_DATE,
                         SECU_CODE,
                         SECU_TYPE_CODE,
                         SYMBOL,
                         SECU_NAME,
                         INVES_CLS_CODE,
                         TX_MKT_CODE, RMN_COST, SHADOW_MKV
                         """

        df = cls.query(sql)
        if not df.empty:
            df = df.rename(columns=cls.etl_rename_dict)
            df[Dimension.HELD_TO_MATURITY] = df[Dimension.HELD_TO_MATURITY].apply(
                lambda x: True if x == "H" else False
            )
            df[Dimension.TRADE_DATE] = df[Dimension.TRADE_DATE].apply(date_to_str)
            df[Measure.MARKET_VALUE] = df[Measure.MARKET_VALUE].fillna(0).astype(float)
            df[Measure.QUANTITY] = df[Measure.QUANTITY].fillna(0).astype(float)
            df[Dimension.INSTRUMENT_NAME] = df[Dimension.INSTRUMENT_NAME].fillna('')
        return df

    @classmethod
    def fetch_data(cls,
                   secu_code: str = None,
                   start_date: Optional[Union[datetime, date]] = date(2020, 1, 1),
                   end_date: Optional[Union[datetime, date]] = None):
        """汇总持仓表"""
        df = cls.get_pos_dtl_data(secu_code=secu_code, start_date=start_date, end_date=end_date)
        if df.empty:
            frame_log.error(f'{cls.__name__}获取组合持仓明细表为空')
            return pd.DataFrame()

        return df


if __name__ == '__main__':
    BondPosition.run_etl()
