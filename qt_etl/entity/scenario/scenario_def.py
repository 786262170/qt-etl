from datetime import date, datetime
from typing import Optional, Union

import pandas as pd

from qt_common.db_manager import pd_read_sql
from qt_common.error import QtException
from qt_common.qt_logging import frame_log
from qt_common.utils import date_to_str
from qt_etl.entity.fields import Dimension
from qt_etl.entity.scenario.scenario import Scenario

__all__ = ["ScenarioDef"]


class ScenarioDef(Scenario):
    """场景因子定义"""

    @classmethod
    def fetch_data(cls,
                   secu_code: str = None,
                   start_date: Optional[Union[datetime, date]] = None,
                   end_date: Optional[Union[datetime, date]] = None):
        df = cls.get_curve_scenario(secu_code, start_date, end_date)
        df_other = cls.get_other_scenario(secu_code, start_date, end_date)
        return pd.concat([df, df_other])

    @classmethod
    def get_curve_scenario(cls,
                           secu_code: str = None,
                           start_date: Optional[Union[datetime, date]] = None,
                           end_date: Optional[Union[datetime, date]] = None):
        # PLATO_CFG_SCENE_FACTOR_REL_CURVE.Type  1-原始值 2-变动值
        sql = f"""
        SELECT
            T1.ID,
            T4.FACTOR_CODE,
            T2.FACTOR_TYPE,
            T2.FACTOR_NAME,
            T2.MODIFICATION_TYPE,
            T2.TRADING_DATE,
            T3.ZERO_MONTH_DATA,
            T3.ONE_MONTH_DATA,
            T3.TWO_MONTH_DATA,
            T3.THREE_MONTH_DATA,
            T3.SIX_MONTH_DATA,
            T3.NINE_MONTH_DATA,
            T3.ONE_YEAR_DATA,
            T3.TWO_YEAR_DATA,
            T3.THREE_YEAR_DATA,
            T3.FOUR_YEAR_DATA,
            T3.FIVE_YEAR_DATA,
            T3.SIX_YEAR_DATA,
            T3.SEVEN_YEAR_DATA,
            T3.EIGHT_YEAR_DATA,
            T3.NINE_YEAR_DATA,
            T3.TEN_YEAR_DATA,
            T3.FIFTEEN_YEAR_DATA,
            T3.TWENTY_YEAR_DATA,
            T3.THIRTY_YEAR_DATA,
            T3.FORTY_YEAR_DATA,
            T3.FIFTY_YEAR_DATA
        FROM
            PLATO_CFG_SCENE T1
        INNER JOIN PLATO_CFG_SCENE_FACTOR_SINGLE_REL T2 ON
            T1.ID = T2.SCENE_ID
        INNER JOIN PLATO_CFG_SCENE_FACTOR_REL_CURVE T3 ON
            T3.SCENE_FACTOR_REL_ID = T2.ID
        INNER JOIN PLATO_CFG_FACTOR T4 ON
            T4.ID = T2.FACTOR_ID
        WHERE T3.TYPE = 2

        """
        try:
            df = pd_read_sql(sql, session="plato_appl")
        except Exception as e:
            frame_log.error(e)
            raise QtException(msg=e)
        if len(df):
            # 列转行
            df = df.rename(columns={
                'TRADING_DATE': Dimension.TRADE_DATE,
                'ID': Dimension.SCENE_ID,
                'MODIFICATION_TYPE': Dimension.MODIFICATION_TYPE,
                'FACTOR_CODE': Dimension.INDEX,
                'FACTOR_TYPE': Dimension.FACTOR_TYPE,
                'FACTOR_NAME': Dimension.FACTOR_NAME,
                'ZERO_MONTH_DATA': '0Y', 'ONE_MONTH_DATA': '0.08Y', 'TWO_MONTH_DATA': '0.17Y',
                'THREE_MONTH_DATA': '0.25Y',
                'SIX_MONTH_DATA': '0.5Y', 'NINE_MONTH_DATA': '0.75Y', 'ONE_YEAR_DATA': '1Y',
                'TWO_YEAR_DATA': '2Y',
                'THREE_YEAR_DATA': '3Y', 'FOUR_YEAR_DATA': '4Y', 'FIVE_YEAR_DATA': '5Y',
                'SIX_YEAR_DATA': '6Y',
                'SEVEN_YEAR_DATA': '7Y', 'EIGHT_YEAR_DATA': '8Y', 'NINE_YEAR_DATA': '9Y',
                'TEN_YEAR_DATA': '10Y',
                'FIFTEEN_YEAR_DATA': '15Y', 'TWENTY_YEAR_DATA': '20Y', 'THIRTY_YEAR_DATA': '30Y',
                'FORTY_YEAR_DATA': '40Y',
                'FIFTY_YEAR_DATA': '50Y'
            })
            df = df.set_index(
                [Dimension.TRADE_DATE, Dimension.SCENE_ID, Dimension.INDEX, Dimension.FACTOR_TYPE,
                 Dimension.FACTOR_NAME, Dimension.MODIFICATION_TYPE]).stack().reset_index()
            df.columns = [Dimension.TRADE_DATE, Dimension.SCENE_ID, Dimension.INDEX, Dimension.FACTOR_TYPE,
                          Dimension.FACTOR_NAME,
                          Dimension.MODIFICATION_TYPE,
                          Dimension.TENOR,
                          Dimension.MODIFICATION_VALUE]
            df = df.astype({Dimension.MODIFICATION_VALUE: float})
            df[Dimension.TRADE_DATE] = df[Dimension.TRADE_DATE].apply(cls.apply_trading_date)

        return df

    @classmethod
    def get_other_scenario(cls,
                           secu_code: str = None,
                           start_date: Optional[Union[datetime, date]] = None,
                           end_date: Optional[Union[datetime, date]] = None):
        sql = """SELECT
                T1.ID,
                T3.FACTOR_CODE ,
                T2.FACTOR_TYPE,
                T2.FACTOR_NAME,
                T2.MODIFICATION_TYPE,
                T2.MODIFICATION_VALUE ,
                T2.TRADING_DATE
            FROM
                PLATO_CFG_SCENE T1
            INNER JOIN PLATO_CFG_SCENE_FACTOR_SINGLE_REL T2 ON
                T1.ID = T2.SCENE_ID
            INNER JOIN PLATO_CFG_FACTOR T3 ON
                T3.ID = T2.FACTOR_ID
            WHERE
                T2.FACTOR_TYPE <> 2"""
        try:
            df = pd_read_sql(sql, session="plato_appl")
        except Exception as e:
            frame_log.error(e)
            raise QtException(msg=e)
        if len(df):
            df = df.rename(columns={
                'TRADING_DATE': Dimension.TRADE_DATE,
                'ID': Dimension.SCENE_ID,
                'MODIFICATION_TYPE': Dimension.MODIFICATION_TYPE,
                'MODIFICATION_VALUE': Dimension.MODIFICATION_VALUE,
                'FACTOR_TYPE': Dimension.FACTOR_TYPE,
                'FACTOR_NAME': Dimension.FACTOR_NAME,
                'FACTOR_CODE': Dimension.INDEX
            })
            df[Dimension.TRADE_DATE] = df[Dimension.TRADE_DATE].apply(cls.apply_trading_date)
        return df

    @staticmethod
    def apply_trading_date(v):
        if isinstance(v, str):
            if "-" in v:
                v = v.replace("-", "")
        elif isinstance(v, (date, datetime)):
            v = date_to_str(v)

        return v


if __name__ == '__main__':
    df = ScenarioDef.run_etl()
