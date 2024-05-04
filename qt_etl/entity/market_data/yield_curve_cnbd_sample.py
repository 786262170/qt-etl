"""中债曲线样本券数据"""
import pandas as pd
import functools
import inspect

from qt_etl.entity.market_data.market_data import MarketData
from qt_etl.entity.fields import Dimension
from qt_etl.entity.portfolio.comb_position import resource_decorator
from qt_common.qt_logging import frame_log as logger
from qt_common.utils import date_to_str

from typing import Optional, Union, List
from datetime import date, datetime
import pyarrow as pa


class YieldCurveCNBDSample(MarketData):
    """债券曲线样本券数据"""
    schema = pa.schema([
        pa.field(Dimension.INDEX, pa.string(),
                 metadata={b'table_field': b'CURV_CODE', b'table_name': b'INFO_FI_YC_CNBD_SAMPLE'}),
        pa.field(Dimension.INDEX_NAME, pa.string(),
                 metadata={b'table_field': b'CURV_CNAME', b'table_name': b'INFO_FI_YC_CNBD_SAMPLE'}),
        pa.field(Dimension.INSTRUMENT_CODE, pa.string(),
                 metadata={b'table_field': b'BOND_CODE', b'table_name': b'INFO_FI_YC_CNBD_SAMPLE'}),
        pa.field(Dimension.TRADE_DATE, pa.string(),
                 metadata={b'table_field': b'TRD_DATE', b'table_name': b'INFO_FI_YC_CNBD_SAMPLE'}),
    ],
        metadata={
            Dimension.INDEX: '曲线代码',
            Dimension.INDEX_NAME: '曲线名称',
            Dimension.INSTRUMENT_CODE: '债券代码',
            Dimension.TRADE_DATE: '交易日期'
        })

    @classmethod
    def get_curve_code(cls):
        """获取情景表中的曲线"""
        sql = """select  FACTOR_CODE from PLATO_CFG_FACTOR where FACTOR_TYPE =2 """
        df = cls.query(sql)
        curve_code = []
        if not df.empty:
            curve_code = df['FACTOR_CODE'].unique().tolist()
        return curve_code



    @classmethod
    def fetch_data(cls,
                   secu_code: str = None,
                   start_date: Optional[Union[datetime, date]] = None,
                   end_date: Optional[Union[datetime, date]] = None):
        sql = f"""
            select
                CURV_CODE,
                CURV_CNAME,
                BOND_CODE,
                TRD_DATE
            from
                INFO_FI_YC_CNBD_SAMPLE
            where
                TRD_DATE BETWEEN '{start_date}' AND '{end_date}'
        """
        if secu_code:
            sql = f"{sql} and BOND_CODE in ({secu_code}) "
        df = cls.query(sql)
        logger.info(f'CNBD yield curve sample len:{len(df)}')
        if df.empty:
            return df
        df = df.rename(
            columns={"CURV_CODE": Dimension.INDEX,
                     "CURV_CNAME": Dimension.INDEX_NAME,
                     "BOND_CODE": Dimension.INSTRUMENT_CODE,
                     "TRD_DATE": Dimension.TRADE_DATE}
        )
        df[Dimension.TRADE_DATE] = df[Dimension.TRADE_DATE].apply(date_to_str)
        return df

    @classmethod
    @resource_decorator(secu_type='BOND')
    def run_etl(cls, secu_codes: Optional[list[str]] = None,
                start_date: Optional[Union[datetime, date]] = None,
                end_date: Optional[Union[datetime, date]] = date.today(),
                is_concurrent_query=True,
                is_concurrent_save=False,
                is_init=False):
        super(YieldCurveCNBDSample, cls).run_etl(
            secu_codes=secu_codes,
            start_date=start_date,
            end_date=end_date,
            is_concurrent_save=is_concurrent_save,
            is_concurrent_query=is_concurrent_query,
            is_init=is_init)


def curve_code_decorator(func):
    @functools.wraps(func)
    def decorated(*args, **kwargs):
        call_args = inspect.getcallargs(func, *args, **kwargs)
        # 证券填充
        secu_codes = call_args.get('secu_codes', call_args.get('secu_code')) or []
        start_date = call_args.get('start_date') or date(2020, 1, 1)
        end_date = call_args.get('end_date') or date.today()
        call_args["start_date"] = start_date
        call_args["end_date"] = end_date
        if not secu_codes:
            df_curve = YieldCurveCNBDSample.get_data()
            secu_codes = [] if df_curve.empty else df_curve[Dimension.INDEX].unique().tolist()
            # 获取曲线code
            curve_code_list = YieldCurveCNBDSample.get_curve_code()
            if curve_code_list:
                secu_codes = list(set(secu_codes).union(curve_code_list))
            # secu_codes += ['CBD100222', 'CBD100092']

        if secu_codes:
            # 供sql format直接使用
            if isinstance(secu_codes, str):
                secu_codes = [secu_codes]
                # where xxx in ({secu_code})
            if isinstance(secu_codes, list):
                secu_codes = ",".join(["'%s'" % item for item in secu_codes])
            "secu_codes" in call_args and call_args.update(secu_codes=secu_codes)
            "secu_code" in call_args and call_args.update(secu_code=secu_codes)

        arg_spec = inspect.getfullargspec(func)
        vargs = []
        kws = call_args.pop(arg_spec.varkw, {})
        for arg in arg_spec.args:
            vargs.append(call_args[arg])
        vargs.extend(call_args.get(arg_spec.varargs, []))
        return func(*vargs, **kws)

    return decorated


if __name__ == '__main__':
    YieldCurveCNBDSample.run_etl()
