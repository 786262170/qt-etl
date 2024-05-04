# coding=utf8
"""Etl Model:外汇交易中心收益率曲线
仅Numerix应用会用到
"""
from datetime import date, datetime
from typing import Optional, Union, List

import pyarrow as pa

from qt_common import utils
from qt_etl.entity import fields
from qt_etl.entity.market_data import market_data

__all__ = ["FiYieldCurve"]


class FiYieldCurve(market_data.MarketData):
    """外汇交易中心收益率曲线"""
    schema = pa.schema([
        pa.field(fields.Dimension.TRADE_DATE, pa.string(),
                 metadata={b'table_field': b'TRD_DATE', b'table_name': b'INFO_FI_CFETS_YIELD_CURV'}),
        pa.field(fields.Dimension.STD_TERM, pa.float64(),
                 metadata={b'table_field': b'STD_TERM', b'table_name': b'INFO_FI_CFETS_YIELD_CURV'}),
        pa.field(fields.Measure.YIELD, pa.float64(),
                 metadata={b'table_field': b'YIELD', b'table_name': b'INFO_FI_CFETS_YIELD_CURV'}),
        pa.field(fields.Dimension.CURVE_TYPE, pa.string(),
                 metadata={b'table_field': b'CURV_TYPE', b'table_name': b'INFO_FI_CFETS_YIELD_CURV'}),
        pa.field(fields.Dimension.CURVE_TYPE_NAME, pa.string(),
                 metadata={b'table_field': b'CURV_CNAME', b'table_name': b'INFO_FI_CFETS_YIELD_CURV'}),
        pa.field(fields.Dimension.CURVE_CODE, pa.string(),
                 metadata={b'table_field': b'CURV_CODE', b'table_name': b'INFO_FI_CFETS_YIELD_CURV'}),
    ],
        metadata={
            fields.Dimension.TRADE_DATE: '交易日期',
            fields.Dimension.STD_TERM: '标准期限(年)',
            fields.Measure.YIELD: '收益率（%）',
            fields.Dimension.CURVE_TYPE: '曲线类型',
            fields.Dimension.CURVE_TYPE_NAME: '曲线名称',
            fields.Dimension.CURVE_CODE: '曲线编号',
        }
    )

    @classmethod
    def fetch_data(cls, secu_code: Optional[Union[str, List[str]]] = None,
                   start_date: Optional[Union[datetime, date]] = None,
                   end_date: Optional[Union[datetime, date]] = None, **kwargs):
        """查询db数据转换为DataFrame"""
        sql = f"""
        SELECT TRD_DATE, STD_TERM, YIELD, CURV_TYPE, CURV_CNAME, CURV_CODE FROM 
        INFO_FI_CFETS_YIELD_CURV 
        WHERE 
        TRD_DATE between date'{start_date}' and date'{end_date}'"""
        df_res = cls.query(sql, as_format=cls.rename)
        if not df_res.empty:
            df_res[fields.Dimension.TRADE_DATE] = df_res[fields.Dimension.TRADE_DATE].apply(utils.date_to_str)
        return df_res


if __name__ == '__main__':
    df = FiYieldCurve.run_etl()
