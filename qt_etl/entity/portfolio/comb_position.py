# vim set fileencoding=utf-8
"""组合持仓 etl save"""
import functools
import inspect
import typing
from datetime import date, datetime
from typing import Optional, Union, Dict

import pandas as pd
import pyarrow as pa

from qt_common.qt_logging import frame_log
from qt_common.utils import date_to_str, PandasMixin
from qt_etl.entity.fields import Dimension, Measure, Currency
from qt_etl.entity.fields import InstrumentType
from qt_etl.entity.portfolio.portfolio import Portfolio
from qt_etl.utils import CurrExchRateTools

__all__ = ["CombPosition", "resource_decorator"]


class CombPosition(Portfolio):
    """汇总组合持仓"""
    main_table = 'INDIC_BASE_PORT_POS_DTL'
    schema = pa.schema([
        pa.field(Dimension.BOOK_ID, pa.string(), metadata={b"table_field": b"PRD_CODE"}),
        pa.field(Dimension.CURRENCY, pa.string(), metadata={b"table_field": b"CUR_CODE"}),
        pa.field(Dimension.TRADE_DATE, pa.string(), metadata={b"table_field": b"BIZ_DATE"}),
        pa.field(Dimension.INSTRUMENT_CODE, pa.string(), metadata={b"table_field": b"SECU_CODE"}),
        pa.field(Dimension.SECU_TYPE_CODE, pa.string(), metadata={b"table_field": b"SECU_TYPE_CODE"}),
        pa.field(Dimension.SYMBOL, pa.string(), metadata={b"table_field": b"SYMBOL"}),
        pa.field(Dimension.INSTRUMENT_NAME, pa.string(), metadata={b"table_field": b"SECU_NAME"}),
        pa.field(Measure.UNIT_COST, pa.float64(), metadata={b"table_field": b"UNIT_COST"}),
        pa.field(Measure.VALUATION_PRICE, pa.float64(), metadata={b"table_field": b"VAL_PRC"}),
        pa.field(Measure.COST, pa.float64(), metadata={b"table_field": b"POS_COST"}),
        pa.field(Measure.MARKET_VALUE, pa.float64(), metadata={b"table_field": b"POS_MKV"}),
        pa.field(Measure.QUANTITY, pa.float64(), metadata={b"table_field": b"POS_QTY"}),
        # shift
        pa.field(Measure.PREV_QUANTITY, pa.float64(), metadata={b"table_field": b""}),
        pa.field(Measure.PREV_MARKET_VALUE, pa.float64(), metadata={b"table_field": b""}),
        pa.field(Measure.PREV_COST, pa.float64(), metadata={b"table_field": b""}),
        pa.field(Dimension.HELD_TO_MATURITY, pa.bool_(), metadata={b"table_field": b"INVES_CLS_CODE"}),
        pa.field(Dimension.INSTRUMENT_TYPE, pa.string(), metadata={b"table_field": b"AST_BIG_CLS_CODE"}),
        pa.field(Dimension.TRADING_MARKET, pa.string(), metadata={b"table_field": b"TX_MKT_CODE"}),
        pa.field(Dimension.REMN_TERM, pa.float64(),
                 metadata={b"table_field": b'REMN_TERM', b'table_name': b'INDIC_BASE_REPO_POS_DTL'}),
    ])
    etl_rename_dict = {
        'PRD_CODE': Dimension.BOOK_ID,
        'CUR_CODE': Dimension.CURRENCY,
        'BIZ_DATE': Dimension.TRADE_DATE,
        'SECU_CODE': Dimension.INSTRUMENT_CODE,
        'SECU_NAME': Dimension.INSTRUMENT_NAME,
        'AST_BIG_CLS_CODE': Dimension.INSTRUMENT_TYPE,
        'SYMBOL': Dimension.SYMBOL,
        'SECU_TYPE_CODE': Dimension.SECU_TYPE_CODE,
        'POS_MKV': Measure.MARKET_VALUE,
        'POS_QTY': Measure.QUANTITY,
        'UNIT_COST': Measure.UNIT_COST,
        'POS_COST': Measure.COST,
        'VAL_PRC': Measure.VALUATION_PRICE,
        'INVES_CLS_CODE': Dimension.HELD_TO_MATURITY,
        'TX_MKT_CODE': Dimension.TRADING_MARKET,
    }

    @classmethod
    def get_pos_dtl_data(cls,
                         secu_code: str = None,
                         start_date: Optional[Union[datetime, date]] = date(2020, 1, 1),
                         end_date: Optional[Union[datetime, date]] = None):
        """
        组合持仓明细表
        :param secu_code:
        :param start_date:
        :param end_date:
        :return:
        """
        # LAST_DAY_POS_QTY, LAST_DAY_POS_MKV, LAST_DAY_POS_COST不从表里读取
        sql = f"""select
                         PRD_CODE,
                         CUR_CODE,
                         BIZ_DATE,
                         SECU_CODE,
                         SECU_TYPE_CODE,
                         SYMBOL,
                         SECU_NAME,
                         max(UNIT_COST) as UNIT_COST,
                         max(VAL_PRC) as VAL_PRC,
                         case when sum(POS_QTY) =0 then 0 else  sum(POS_QTY * POS_COST)/ sum(POS_QTY) end as POS_COST ,
                         sum(POS_MKV) as POS_MKV, sum(POS_QTY) as POS_QTY,
                         INVES_CLS_CODE, AST_BIG_CLS_CODE, TX_MKT_CODE 
                     FROM
                         INDIC_BASE_PORT_POS_DTL
                     WHERE
                         BIZ_DATE between '{start_date}' and '{end_date}'
          --                切换新表去掉此筛选条件
                         AND DOWN_FLAG='0'
                     group by
                         PRD_CODE,
                         CUR_CODE,
                         BIZ_DATE,
                         SECU_CODE,
                         SECU_TYPE_CODE,
                         SYMBOL,
                         SECU_NAME,
                         INVES_CLS_CODE,
                         AST_BIG_CLS_CODE,
                         TX_MKT_CODE
                         """

        df = cls.query(sql)
        if not df.empty:
            df = df.rename(columns=cls.etl_rename_dict)

            df[Dimension.HELD_TO_MATURITY] = df[Dimension.HELD_TO_MATURITY].apply(
                lambda x: True if x == "H" else False
            )
            df[Dimension.TRADE_DATE] = df[Dimension.TRADE_DATE].apply(date_to_str)
            df[Measure.MARKET_VALUE] = df[Measure.MARKET_VALUE].fillna(0).astype(float)
            df[Measure.QUANTITY] = df[Measure.QUANTITY].fillna(0).astype(float)
            df[Dimension.INSTRUMENT_NAME] = df[Dimension.INSTRUMENT_NAME].fillna('')
            df[Dimension.INSTRUMENT_TYPE] = df[Dimension.INSTRUMENT_TYPE].replace(to_replace='CASH_OTHER',
                                                                                  value=InstrumentType.CASH.name)
        return df

    @staticmethod
    def shift_data(df):
        """
        获取LAST_DAY_POS_QTY, LAST_DAY_POS_MKV, LAST_DAY_POS_COST
        """
        if df.empty:
            return df

        df = df.sort_values(by=Dimension.TRADE_DATE)
        shift_columns = [Measure.QUANTITY, Measure.MARKET_VALUE, Measure.COST]
        res_shift_columns = [Measure.PREV_QUANTITY, Measure.PREV_MARKET_VALUE, Measure.PREV_COST]

        group_columns = [Dimension.BOOK_ID, Dimension.INSTRUMENT_CODE]
        df[res_shift_columns] = df.groupby(group_columns)[shift_columns].shift()

        return df

    @classmethod
    def fetch_data(cls,
                   secu_code: str = None,
                   start_date: Optional[Union[datetime, date]] = date(2020, 1, 1),
                   end_date: Optional[Union[datetime, date]] = None):
        """汇总持仓表"""
        df = cls.get_pos_dtl_data(secu_code=secu_code, start_date=start_date, end_date=end_date)
        if df.empty:
            frame_log.error(f'{cls.__name__}获取组合持仓明细表为空')
            return pd.DataFrame()
        # 获取last信息
        df = cls.shift_data(df)

        df_repo_weighted_maturity = cls._get_repo_weighted_maturity(secu_code, start_date=start_date, end_date=end_date)
        if not df_repo_weighted_maturity.empty:
            df = df.merge(df_repo_weighted_maturity, how='left',
                          on=[Dimension.BOOK_ID, Dimension.TRADE_DATE, Dimension.INSTRUMENT_CODE])
        if Dimension.REMN_TERM in df.columns:
            df[Dimension.REMN_TERM] = df[Dimension.REMN_TERM].fillna(0.0)
        else:
            df[Dimension.REMN_TERM] = 0.0
        df = df.reset_index(drop=True)
        return df

    @classmethod
    def _get_repo_weighted_maturity(cls,
                                    secu_code: str = None,
                                    start_date: Optional[Union[datetime, date]] = date(2020, 1, 1),
                                    end_date: Optional[Union[datetime, date]] = None):
        sql = f"""SELECT PRD_CODE, BIZ_DATE, SECU_CODE, REMN_TERM, POS_COST
                               FROM INDIC_BASE_REPO_POS_DTL 
                               WHERE BIZ_DATE between '{start_date}' and '{end_date}'"""
        if secu_code:
            sql += f""" where SECU_CODE in ({secu_code})"""
        df = cls.query(sql)
        if not df.empty:
            df['REMN_TERM'] = df['REMN_TERM'].astype(float)
            df['BIZ_DATE'] = df['BIZ_DATE'].apply(date_to_str)
            df['TIME_WEIGHTED_COST'] = df['REMN_TERM'] * df['POS_COST']

            g = df.groupby(['PRD_CODE', 'BIZ_DATE', 'SECU_CODE'])
            result = g['TIME_WEIGHTED_COST'].sum() / g['POS_COST'].sum()
            df = result.reset_index()
            df = df.rename(columns={
                'PRD_CODE': Dimension.BOOK_ID,
                'BIZ_DATE': Dimension.TRADE_DATE,
                'SECU_CODE': Dimension.INSTRUMENT_CODE,
                0: Dimension.REMN_TERM
            })
        return df

    @staticmethod
    def get_secu_codes(df, secu_type: Optional[Union[str, typing.Iterable]] = None):
        """获取对应证券分类下的证券代码列表"""
        if df.empty:
            return []
        if not secu_type:
            return df[Dimension.INSTRUMENT_CODE].unique().tolist()
        else:
            sec_codes = []
            pos_mapper = PandasMixin.df_group(
                df, by=[Dimension.INSTRUMENT_TYPE], include=secu_type)
            for sec_type, g in pos_mapper.items():
                g_sec_codes = g[Dimension.INSTRUMENT_CODE].unique().tolist()
                sec_codes.extend(g_sec_codes)
        return sec_codes

    @staticmethod
    def filter_bond_secu_type(df: pd.DataFrame):
        return df[df[Dimension.BOND_TYPE].str.contains("A02.02")]

    @staticmethod
    def apply_curr_exchange(row: pd.Series, req_cuur: Union[str, Currency],
                            exch_rate_map: Dict[str, float]):
        """对持仓数组种每一行市值进行汇率换算-单个日期下分组持仓

        :param row: 持仓记录[单条]
        :param req_cuur: 请求货币字段
        :param exch_rate_map: 汇率映射字典
        :return: 更新后的持仓记录[单条]
        """
        row[Measure.MARKET_VALUE] *= CurrExchRateTools.calc_curr_exch_rate(
            str(req_cuur), row[Dimension.CURRENCY], exch_rate_map)
        return row

    @staticmethod
    def apply_many_curr_exchange(row: pd.Series, req_cuur: Union[str, Currency],
                                 date_exch_rate_map: Dict[str, Dict[str, float]]):
        """对持仓数组种每一行市值进行汇率换算-多个日期下持仓未分组

        :param row: 持仓记录[单条]
        :param req_cuur: 请求货币字段
        :param date_exch_rate_map: 日期汇率映射字典Dict[str:Dict[str:float]]
        :return: 更新后的持仓记录[单条]
        """
        exch_rate_map = date_exch_rate_map.get(row[Dimension.TRADE_DATE], {})
        row[Measure.MARKET_VALUE] *= CurrExchRateTools.calc_curr_exch_rate(
            str(req_cuur), row[Dimension.CURRENCY], exch_rate_map)
        return row


def resource_decorator(secu_type: Optional[Union[str, typing.Iterable]] = None, **extra):
    """组合证券填充

    对run_etl和fetch_data等函数提供预置资源获取，对特定过滤字段比如secu_codes or secu_code提供
    sql格式化字符串处理, 对secu_type字段默认过滤持仓类别
    :examples
    >>> @classmethod
    >>> @resource_decorator(secu_type="BOND")
    >>> def fetch_data(cls,
        >>> secu_code: str = None,
        >>> start_date: Optional[Union[datetime, date]] = None,
        >>> end_date: Optional[Union[datetime, date]] = None):
    """

    def _resource_decorator(func):
        @functools.wraps(func)
        def _wrapper(*args, **kwargs):
            call_args = inspect.getcallargs(func, *args, **kwargs)
            # 证券填充
            secu_codes = call_args.get('secu_codes', call_args.get('secu_code')) or []
            start_date = call_args.get('start_date') or date(2020, 1, 1)
            end_date = call_args.get('end_date') or date.today()
            call_args["start_date"] = start_date
            call_args["end_date"] = end_date
            if not secu_codes:
                secu_codes = CombPosition.get_data(
                    decoder=lambda df: CombPosition.get_secu_codes(df, secu_type))

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

        return _wrapper

    return _resource_decorator


if __name__ == '__main__':
    CombPosition.run_etl()
    df = CombPosition.get_data()
    print(df)
