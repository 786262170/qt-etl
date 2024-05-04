# vim set fileencoding=utf-8
from datetime import date, datetime
from typing import Optional, Union

from qt_common.utils import date_to_str
from qt_etl.entity.factor.factor import Factor
from qt_etl.entity.fields import Dimension

__all__ = ['BarraLevel3Mean']


class BarraLevel3Mean(Factor):
    """Barras三级因子平均值"""

    @classmethod
    def fetch_data(cls,
                   secu_code: str = None,
                   start_date: Optional[Union[datetime, date]] = None,
                   end_date: Optional[Union[datetime, date]] = None):
        sql = f"""SELECT * FROM INFO_BARRA_LEVEL3_MEAN"""
        df = cls.query(sql, upper_columns=False)
        type_dict = {
            Dimension.TRADE_DATE: 'datetime64',
        }
        df = df.astype(dtype=type_dict)

        df[Dimension.TRADE_DATE] = df[Dimension.TRADE_DATE].apply(date_to_str)
        return df


if __name__ == '__main__':
    BarraLevel3Mean.run_etl()
