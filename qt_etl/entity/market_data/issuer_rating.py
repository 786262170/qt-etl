# vim set fileencoding=utf-8
"""主体信用评级表"""
from datetime import date, datetime
from typing import Optional, Union

import pandas as pd

from qt_common.qt_logging import frame_log
from qt_common.utils import date_to_str
from qt_etl.entity.fields import Dimension
from qt_etl.entity.market_data.market_data import MarketData
from qt_common.error import QtException

__all__ = ['IssuerRating']


class IssuerRating(MarketData):
    """发行主体评级"""
    etl_rename_dict = {
        'PARTY_CODE': Dimension.ISSUER_CODE,
        'CREDIT_RAT': Dimension.RATING,
        'ANC_DATE': Dimension.TRADE_DATE,
        'RAT_ORG': Dimension.RATING_ORGANIZATION
    }

    @classmethod
    def fetch_data(cls,
                   secu_code: str = None,
                   start_date: Optional[Union[datetime, date]] = None,
                   end_date: Optional[Union[datetime, date]] = None):
        sql = f"""SELECT PARTY_CODE, CREDIT_RAT, ANC_DATE, RAT_ORG FROM INFO_PARTY_RATING"""
        df = cls.query(sql)
        if not df.empty:
            df = df.rename(columns=cls.etl_rename_dict)
            df[Dimension.TRADE_DATE] = df[Dimension.TRADE_DATE].apply(date_to_str)
        return df

    @classmethod
    def append_rating(cls, df):
        # df.issuer_code, and df.trade_date >= df_issuer_rating.start_date过滤，然后选择最后一个（日期最近的）
        try:
            if not df.empty and Dimension.TRADE_DATE in df.columns:
                dfs = []
                df_issuer_rating = cls.get_data()
                for d, g in df.groupby(Dimension.TRADE_DATE):
                    df_t = df_issuer_rating[df_issuer_rating[Dimension.TRADE_DATE] <= d]
                    df_t = df_t.drop_duplicates(subset=[Dimension.ISSUER_CODE], keep='last')
                    df_t = df_t[[Dimension.ISSUER_CODE, Dimension.RATING]]
                    dfs.append(g.merge(df_t, how='left', on=[Dimension.ISSUER_CODE]))
                if dfs:
                    return pd.concat(dfs)
            else:
                return df
        except Exception as e:
            raise QtException(msg=f'{cls.__name__} append_rating error:{e}')


if __name__ == '__main__':
    IssuerRating.run_etl()
