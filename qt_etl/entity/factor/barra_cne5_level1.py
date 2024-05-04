# vim set fileencoding=utf-8
import time
from datetime import date, datetime
from typing import Optional, Union

from qt_common.utils import date_to_str
from qt_etl.entity.factor.factor import Factor
from qt_etl.entity.fields import Dimension

__all__ = ['BarraCne5Level1']


class BarraCne5Level1(Factor):
    """Barra CNE5因子"""

    @classmethod
    def fetch_data(cls,
                   secu_code: str = None,
                   start_date: Optional[Union[datetime, date]] = date(2020, 1, 1),
                   end_date: Optional[Union[datetime, date]] = datetime.now().date()):
        # 数据库存了十年的数据(默认etl从2020年开始
        sql = f"""SELECT * FROM INFO_BARRA_EXPOSURE_CNE5 where trade_date between '{start_date}' and '{end_date}'"""
        df = cls.query(sql, upper_columns=False)
        if not df.empty:
            df = df.drop(columns=['create_time', 'update_time'])
            df[Dimension.TRADE_DATE] = df[Dimension.TRADE_DATE].apply(date_to_str)
        return df

    @classmethod
    def run_etl(cls, **kwargs):
        extra = {
            "is_concurrent_query": True,
        }
        kwargs.update(extra)
        return super(BarraCne5Level1, cls).run_etl(**kwargs)


if __name__ == '__main__':
    start_time = time.time()
    df = BarraCne5Level1.run_etl()
    print(f'total time:{time.time() - start_time}')
