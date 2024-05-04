# vim set fileencoding=utf-8
"""主体信用评级表"""
from datetime import date, datetime
from typing import Optional, Union
import pandas as pd

from qt_common.utils import date_to_str
from qt_etl.entity.fields import Dimension
from qt_etl.entity.market_data.market_data import MarketData
from qt_common.error import QtException

__all__ = ['InstrumentRating']


class InstrumentRating(MarketData):
    """产品信用评级"""
    etl_rename_dict = {
        'BOND_CODE': Dimension.INSTRUMENT_CODE,
        'CREDIT_RAT': Dimension.RATING,
        'ANC_DATE': Dimension.TRADE_DATE,
        'RAT_ORG': Dimension.RATING_ORGANIZATION
    }

    @classmethod
    def fetch_data(cls,
                   secu_code: str = None,
                   start_date: Optional[Union[datetime, date]] = None,
                   end_date: Optional[Union[datetime, date]] = None):
        sql = f"""SELECT BOND_CODE, CREDIT_RAT, ANC_DATE, RAT_ORG FROM INFO_FI_BONDRATING
                WHERE RAT_ORG in ('002', '003', '004', '005', '006', '034', '013', '014', '019')"""
        df = cls.query(sql)
        if not df.empty:
            df = df.rename(columns=cls.etl_rename_dict)
            df[Dimension.TRADE_DATE] = df[Dimension.TRADE_DATE].apply(date_to_str)
        return df

    @classmethod
    def append_rating(cls, df):
        # df.issuer_code, and df.trade_date >= df_instrument_rating.start_date过滤，然后选择最后一个（日期最近的）
        try:
            df_instrument_rating = cls.get_data()
            dfs = []
            for d, g in df.groupby(Dimension.TRADE_DATE):
                df_t = df_instrument_rating[df_instrument_rating[Dimension.TRADE_DATE] <= d]
                df_t = df_t.drop_duplicates(subset=[Dimension.INSTRUMENT_CODE], keep='last')
                df_t = df_t[[Dimension.INSTRUMENT_CODE, Dimension.RATING]]
                dfs.append(g.merge(df_t, how='left', on=[Dimension.INSTRUMENT_CODE]))
            if dfs:
                rating_df = pd.concat(dfs)
                rating_df[Dimension.RATING_TYPE] = rating_df[Dimension.RATING].apply(cls.get_rating_type)
            else:
                rating_df = pd.DataFrame()
            return rating_df
        except QtException:
            raise
        except Exception as e:
            raise QtException(msg=f'{cls.__name__} append_rating error:{e}')

    @classmethod
    def get_rating_type(cls, rating):
        if not rating:
            curve_rating = None
        elif rating in ['AAA+', 'AAA', 'AAA-']:
            curve_rating = 'AAA'
        elif rating == 'AA+':
            curve_rating = 'AA+'
        else:
            curve_rating = 'AA'

        return curve_rating

if __name__ == '__main__':
    InstrumentRating.run_etl()
