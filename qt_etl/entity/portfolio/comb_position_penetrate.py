# vim set fileencoding=utf-8
"""组合持仓 etl save"""
from datetime import date, datetime
from typing import Optional, Union

import pandas as pd
import pyarrow as pa

from qt_common.qt_logging import frame_log
from qt_etl.entity.fields import Dimension, Measure
from qt_etl.entity.portfolio import CombPosition
from qt_etl.entity.portfolio import DownRelationPortfolio
from qt_etl.entity.portfolio.portfolio import Portfolio

__all__ = ["CombPositionPenetrate"]


class CombPositionPenetrate(Portfolio):
    """穿透后汇总组合持仓"""
    main_table = 'INDIC_BASE_PORT_POS_DTL'
    schema = pa.schema([
        pa.field(Dimension.BOOK_ID, pa.string(), metadata={b"table_field": b"PRD_CODE"}),
        pa.field(Dimension.CURRENCY, pa.string(), metadata={b"table_field": b"CUR_CODE"}),
        pa.field(Dimension.TRADE_DATE, pa.string(), metadata={b"table_field": b"BIZ_DATE"}),
        pa.field(Dimension.INSTRUMENT_CODE, pa.string(), metadata={b"table_field": b"SECU_CODE"}),
        pa.field(Dimension.SECU_TYPE_CODE, pa.string(), metadata={b"table_field": b"SECU_TYPE_CODE"}),
        pa.field(Dimension.SYMBOL, pa.string(), metadata={b"table_field": b"SYMBOL"}),
        pa.field(Dimension.INSTRUMENT_NAME, pa.string(), metadata={b"table_field": b"SECU_NAME"}),
        pa.field(Dimension.INSTRUMENT_TYPE, pa.string(), metadata={b"table_field": b"AST_BIG_CLS_CODE"}),
        pa.field(Measure.MARKET_VALUE, pa.float64(), metadata={b"table_field": b"POS_MKV"}),
        pa.field(Measure.QUANTITY, pa.float64(), metadata={b"table_field": b"POS_QTY"}),
        pa.field(Measure.UNIT_COST, pa.float64(), metadata={b"table_field": b"UNIT_COST"}),
        pa.field(Measure.COST, pa.float64(), metadata={b"table_field": b"POS_COST"}),
        pa.field(Measure.PREV_QUANTITY, pa.float64(), metadata={b"table_field": b""}),
        pa.field(Measure.PREV_MARKET_VALUE, pa.float64(), metadata={b"table_field": b""}),
        pa.field(Measure.PREV_COST, pa.float64(), metadata={b"table_field": b""}),
        pa.field(Measure.VALUATION_PRICE, pa.float64(), metadata={b"table_field": b"VAL_PRC"}),
        pa.field(Dimension.HELD_TO_MATURITY, pa.bool_(), metadata={b"table_field": b"INVES_CLS_CODE"}),
        pa.field(Dimension.TRADING_MARKET, pa.string(), metadata={b"table_field": b"TX_MKT_CODE"}),
        pa.field(Dimension.REMN_TERM, pa.float64(),
                 metadata={b"table_field": b'REMN_TERM', b'table_name': b'INDIC_BASE_REPO_POS_DTL'}),
    ])

    @classmethod
    def fetch_data(cls,
                   secu_code: str = None,
                   start_date: Optional[Union[datetime, date]] = date(2020, 1, 1),
                   end_date: Optional[Union[datetime, date]] = None):
        # 获取组合持仓明细（穿透前）
        pos_df = CombPosition.get_pos_dtl_data(secu_code=secu_code, start_date=start_date, end_date=end_date)
        if pos_df.empty:
            frame_log.warning('获取持仓明细数据为空')
            return pd.DataFrame()

        # 获取穿透后数据
        penetrate_df = DownRelationPortfolio.get_penetrate_data(start_date=start_date, end_date=end_date)
        if penetrate_df.empty:
            frame_log.warning('获取持仓明细数据为空')
            return pd.DataFrame()
        # 交集
        pos_penetrate_df = pos_df.merge(penetrate_df, left_on=[Dimension.TRADE_DATE, Dimension.BOOK_ID],
                                        right_on=[Dimension.TRADE_DATE, Dimension.CHILDREN_BOOK_ID])

        # 将level1_book_id替换为book_id
        pos_penetrate_df[Dimension.BOOK_ID] = pos_penetrate_df['level1_book_id']

        if pos_penetrate_df.empty:
            frame_log.warning('持仓明细和穿透后交集为空')
            return pd.DataFrame()

        measure_columns = [Measure.MARKET_VALUE, Measure.QUANTITY, Measure.COST]
        pos_penetrate_df[measure_columns] = pos_penetrate_df[measure_columns].mul(pos_penetrate_df[Measure.INVEST_RATE],
                                                                                  axis=0)

        # 避免同一个code 有不同信息
        info_columns = [Dimension.INSTRUMENT_CODE, Dimension.SECU_TYPE_CODE, Dimension.SYMBOL,
                        Dimension.INSTRUMENT_NAME, Dimension.INSTRUMENT_TYPE, Dimension.HELD_TO_MATURITY,
                        Dimension.TRADING_MARKET]
        secu_code_info_df = pos_penetrate_df[info_columns]

        # 判断第一行是否有空值
        if secu_code_info_df.iloc[0].isnull().any():
            secu_code_info_df = secu_code_info_df.fillna(method='bfill')
        # 上一个非空值填充
        secu_code_info_df = secu_code_info_df.fillna(method='ffill')
        secu_code_info_df = secu_code_info_df.groupby(Dimension.INSTRUMENT_CODE).first().reset_index()

        # VAL_PRC, Measure.VALUATION_PRICE, 不变
        group_columns = [Dimension.BOOK_ID, Dimension.CURRENCY, Dimension.TRADE_DATE, Dimension.INSTRUMENT_CODE,
                         Measure.VALUATION_PRICE, Measure.INVEST_RATE]
        pos_penetrate_df = pos_penetrate_df.groupby(group_columns, as_index=False)[measure_columns].sum()
        # sum(POS_COST)/sum(POS_QTY)
        # 穿透后的单位成本 = 穿透后的持仓成本 / 穿透后的持仓数量
        pos_penetrate_df[Measure.UNIT_COST] = pos_penetrate_df[Measure.COST] / pos_penetrate_df[Measure.QUANTITY]

        df = CombPosition.shift_data(pos_penetrate_df)

        df_repo_weighted_maturity = CombPosition._get_repo_weighted_maturity(secu_code, start_date=start_date,
                                                                             end_date=end_date)
        if not df_repo_weighted_maturity.empty:
            df = df.merge(df_repo_weighted_maturity, how='left',
                          on=[Dimension.BOOK_ID, Dimension.TRADE_DATE, Dimension.INSTRUMENT_CODE])

        if not df.empty:
            if Dimension.REMN_TERM in df.columns:
                df[Dimension.REMN_TERM] = df[Dimension.REMN_TERM].fillna(0.0)
            else:
                df[Dimension.REMN_TERM] = 0.0
            df = df.merge(secu_code_info_df, how='left')
            df = df.reset_index(drop=True)
        return df


    @classmethod
    def get_penetrate_trade_data(cls, secu_code: str = None,
                   start_date: Optional[Union[datetime, date]] = None,
                   end_date: Optional[Union[datetime, date]] = None, instrument_type=None):
        # 获取穿透后持仓
        cond = {}
        if instrument_type:
            cond[Dimension.INSTRUMENT_TYPE] = instrument_type
        position_df = cls.get_data(cond=cond)
        if position_df.empty:
            frame_log.warning(f'CombPositionPenetrate数据为空, cond={cond}')
            return pd.DataFrame()
        position_columns = [Dimension.BOOK_ID, Dimension.TRADE_DATE, Measure.COST, Measure.PREV_COST, Measure.QUANTITY,
                            Measure.PREV_QUANTITY, Dimension.INSTRUMENT_CODE]
        df = position_df[position_columns].copy()
        df.loc[:, Measure.TRADE_NOMINAL] = df[Measure.QUANTITY] - df[Measure.PREV_QUANTITY]
        return df




if __name__ == '__main__':
    df = CombPositionPenetrate.run_etl()
