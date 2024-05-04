# vim set fileencoding=utf-8
"""Calendar etl module"""
from datetime import datetime, date
from typing import Optional, Union

import pandas
import pandas as pd

from qt_etl.entity.fields import Dimension
from qt_etl.entity.market_data.market_data import MarketData

__all__ = ["QtCalendar"]


class QtCalendar(MarketData):
    """日历表"""
    partitioned_by_date = False

    @classmethod
    def fetch_data(
            cls,
            secu_code: str = None,
            start_date: Optional[Union[datetime, date]] = None,
            end_date: Optional[Union[datetime, date]] = None,
    ):
        calendars_df = cls.get_calendar()
        calendars_date_df = cls.get_calendar_date()
        if calendars_df.empty:
            return calendars_df
        if calendars_date_df.empty:
            return calendars_df
        inner_df = pd.merge(
            calendars_df, calendars_date_df, how="inner", on="CALENDAR_ID"
        )
        if inner_df.empty:
            return inner_df
        ret = pandas.concat(
            [inner_df, calendars_date_df], keys=['CAL0004', "ALL"]
        ).dropna()
        return ret

    @classmethod
    def get_calendar(cls):
        sql = """select CALENDAR_ID from CALENDARS where IS_DEFAULT_CALENDAR=1"""
        df = cls.query(sql)
        return df

    @classmethod
    def get_calendar_date(cls):
        sql = f"""select CALENDAR_DATE, CALENDAR_ID FROM CALENDAR_DATES  WHERE IS_TRADING_DAY=1 order by
		CALENDAR_DATE """
        df = cls.query(sql)
        return df

    @classmethod
    def get_data(
            cls,
            secu_codes: Optional[list[str]] = None,
            start_date: Optional[Union[date, datetime]] = None,
            end_date: Optional[Union[date, datetime]] = None,
            cond: Optional[dict[Dimension, list]] = None,
            columns: Optional[list] = None,
    ):
        data = super(QtCalendar, cls).get_data(
            secu_codes, cond, columns
        )
        if start_date:
            data = data[(data.CALENDAR_DATE >= start_date)]
        if end_date:
            data = data[(data.CALENDAR_DATE <= end_date)]
        return data

    @staticmethod
    def get_prev_biz_date(trade_date, num=1, calendar_id='CAL0004'):
        if num > 0:
            calendar_df = QtCalendar.get_data(end_date=trade_date)
        else:
            calendar_df = QtCalendar.get_data(start_date=trade_date)
        if calendar_id == 'CAL0004':
            trade_date_df = calendar_df.loc['CAL0004']
        else:
            trade_date_df = calendar_df[calendar_df['CALENDAR_ID'] == calendar_id]
        if len(trade_date_df):
            if num >= 0:
                trade_date_df = trade_date_df.sort_values(by='CALENDAR_DATE', ascending=False)
            else:
                trade_date_df = trade_date_df.sort_values(by='CALENDAR_DATE', ascending=True)
        if num:
            trade_date_df = trade_date_df.iloc[:abs(num) + 1]
        trade_date_list = QtCalendar.as_list(trade_date_df)
        if len(trade_date_list):
            return trade_date_list[-1]
        return None

    @staticmethod
    def get_prev_biz_date_list(trade_date, num=252, calendar_id='CAL0004'):
        calendar_df = QtCalendar.get_data(end_date=trade_date)
        if calendar_id == 'CAL0004':
            trade_date_df = calendar_df.loc['CAL0004']
        else:
            trade_date_df = calendar_df[calendar_df['CALENDAR_ID'] == calendar_id]
        if len(trade_date_df):
            trade_date_df = trade_date_df.sort_values(by='CALENDAR_DATE', ascending=False)
        if num:
            trade_date_df = trade_date_df.iloc[:num + 1]
        trade_date_list = QtCalendar.as_list(trade_date_df)
        if len(trade_date_list):
            return trade_date_list
        return None

    @staticmethod
    def get_trade_date_by_calendar_id(
            start_date=None, end_date=None, calendar_id="CAL0002"
    ):
        trade_date_df = (
            QtCalendar.get_data(start_date=start_date, end_date=end_date)
            .loc["ALL"]
            .sort_index()
        )
        if calendar_id:
            trade_date_df = trade_date_df[trade_date_df.CALENDAR_ID == calendar_id]
        return trade_date_df

    @staticmethod
    def as_list(data):
        return list(data.CALENDAR_DATE.values)


if __name__ == '__main__':
    # QtCalendar.run_etl()
    res = QtCalendar.get_prev_biz_date(date(2022, 1, 1), num=252)
    print(res)
    print(type(res))
