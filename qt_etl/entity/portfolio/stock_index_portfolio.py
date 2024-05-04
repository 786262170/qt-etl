# vim set fileencoding=utf-8

from datetime import date, datetime
from typing import Optional, Union

import pyarrow as pa

from qt_common.utils import date_to_str
from qt_etl.config import get_config
from qt_etl.constants import PartitionByDateType, DEF_SEC_CSI
from qt_etl.entity.fields import Dimension, Measure
from qt_etl.entity.portfolio import resource_decorator
from qt_etl.entity.portfolio.portfolio import Portfolio

__all__ = ['StockIndexPortfolio']


class StockIndexPortfolio(Portfolio):
    """股票指数组合持仓"""
    main_table = 'INFO_IDX_WT_STK'
    partitioned_by_date = PartitionByDateType.month
    # partitioned_cols = [Dimension.BOOK_ID]

    schema = pa.schema([
        pa.field(Dimension.BOOK_ID, pa.string(), metadata={b"table_field": b"IDX_CODE"}),
        pa.field(Dimension.TRADE_DATE, pa.string(), metadata={b"table_field": b"TRD_DATE"}),
        pa.field(Dimension.INSTRUMENT_CODE, pa.string(), metadata={b"table_field": b"CPN_CODE"}),
        pa.field(Measure.QUANTITY, pa.float64(), metadata={b"table_field": b"WT"}),

    ],
        metadata={
            Dimension.BOOK_ID: '指数内部编码',
            Dimension.TRADE_DATE: '交易日期',
            Dimension.INSTRUMENT_CODE: '成份内部编码',
            Measure.QUANTITY: '权重(%)',
        }
    )

    @classmethod
    @resource_decorator()
    def fetch_data(cls,
                   secu_code: str = None,
                   start_date: Optional[Union[datetime, date]] = None,
                   end_date: Optional[Union[datetime, date]] = None):
        sql = f"""SELECT IDX_CODE,
                        TRD_DATE,
                        CPN_CODE,
                        WT
                        FROM INFO_IDX_WT_STK
                        WHERE
                        trd_date BETWEEN '{start_date}' AND '{end_date}'
                        """
        if secu_code and isinstance(secu_code, str):
            sql += f" AND IDX_CODE in ({secu_code})"
        df = cls.query(sql)
        if not df.empty:
            etl_rename_dict = {
                'IDX_CODE': Dimension.BOOK_ID,
                'TRD_DATE': Dimension.TRADE_DATE,
                'CPN_CODE': Dimension.INSTRUMENT_CODE,
                'WT': Measure.QUANTITY
            }
            df = df.rename(columns=etl_rename_dict)
            df[Dimension.TRADE_DATE] = df[Dimension.TRADE_DATE].apply(date_to_str)
            df[Measure.QUANTITY] = df[Measure.QUANTITY] / 100

        return df

    @classmethod
    def run_etl(cls, **kwargs):
        # 过滤参数处理, 默认增加沪深300证券过滤
        secu_codes = kwargs.pop('secu_codes', get_config(
            f"etl_{cls.__name__}", "filter_secu_codes", encode=lambda x: x.split(","),
            default=DEF_SEC_CSI))
        extra = {
            "is_concurrent_query": True,
            "is_concurrent_save": True,
            "secu_codes": secu_codes,
            "is_init": False
        }
        kwargs.update(extra)
        return super().run_etl(**kwargs)


if __name__ == '__main__':
    StockIndexPortfolio.run_etl(start_date=datetime(2021, 1, 1), )
