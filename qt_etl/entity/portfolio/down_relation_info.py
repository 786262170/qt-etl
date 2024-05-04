# coding=utf-8
"""穿透映射关系"""
import copy
import os.path
from datetime import date, datetime
from functools import lru_cache
from typing import Optional, Union, List, Tuple

import numpy as np
import pandas as pd
import pyarrow as pa

from qt_common.error import QtException
from qt_common.qt_logging import frame_log
from qt_common.utils import date_to_str
from qt_etl.entity.fields import Dimension, Measure
from qt_etl.entity.portfolio.portfolio import Portfolio

__all__ = ['DownRelationPortfolio']


class DownRelationPortfolio(Portfolio):
    """穿透映射关系"""
    main_table = 'INDIC_DOWN_RELATION'
    schema = pa.schema([
        pa.field(Dimension.BOOK_ID, pa.string(), metadata={b"table_field": b"PRODUCT_CODE"}),
        pa.field(Dimension.TRADE_DATE, pa.string(), metadata={b"table_field": b"VALUATION_DATE"}),
        pa.field(Dimension.CHILDREN_BOOK_ID, pa.string(), metadata={b"table_field": b"SECU_CODE"}),
        pa.field(Measure.INVEST_RATE, pa.float64(), metadata={b"table_field": b"INVEST_RATE"}),
    ], metadata={
        Dimension.BOOK_ID: '组合名称',
        Dimension.TRADE_DATE: '日期',
        Dimension.CHILDREN_BOOK_ID: '产品代码',
        Measure.INVEST_RATE: '投资占比',
    })
    etl_rename_dict = {
        'PRODUCT_CODE': Dimension.BOOK_ID,
        'VALUATION_DATE': Dimension.TRADE_DATE,
        'SECU_CODE': Dimension.CHILDREN_BOOK_ID,
        'INVEST_RATE': Measure.INVEST_RATE
    }

    @classmethod
    def fetch_data(cls, secu_code: str = None,
                   start_date: Optional[Union[datetime, date]] = None,
                   end_date: Optional[Union[datetime, date]] = None):
        sql = f"""SELECT PRODUCT_CODE, VALUATION_DATE, SECU_CODE, INVEST_RATE FROM INDIC_DOWN_RELATION"""
        df = cls.query(sql)
        df.rename(columns=cls.etl_rename_dict, inplace=True)
        df[Dimension.TRADE_DATE] = df[Dimension.TRADE_DATE].apply(date_to_str)
        df[Dimension.BOOK_ID] = df[Dimension.BOOK_ID].astype(str)
        return df

    @classmethod
    def get_data(cls, *args, **kwargs):
        etl_dir = cls.get_etl_dir()
        if not os.path.exists(etl_dir):
            cls.run_etl()
        return super().get_data(*args, **kwargs)

    @classmethod
    def check_down_flag(cls, query_date: Union[str, List[str], Tuple[str], datetime.date] = None,
                        book_id: str = None, penetrate: str = "0"):
        """依赖函数,根据业务查询是否支持下钻
        """
        frame_log.info("check down flag starting")
        down_flag_df = cls.get_data()
        if isinstance(query_date, (list, tuple)):
            start_date, end_date = query_date
            down_flag_df = down_flag_df.loc[
                (down_flag_df[Dimension.TRADE_DATE] >= start_date) &
                (down_flag_df[Dimension.TRADE_DATE] <= end_date)]
        else:
            if query_date:
                down_flag_df = down_flag_df.loc[(down_flag_df[Dimension.TRADE_DATE] == query_date)]
        if book_id:
            down_flag_df = down_flag_df.loc[down_flag_df[Dimension.BOOK_ID].isin([book_id])]
        if not down_flag_df.empty:
            penetrate = "0"
        cls.down_flag.set(penetrate)
        frame_log.info("check down flag finished.. down_flag={}", penetrate)
        return penetrate

    @classmethod
    def get_penetrate_data_sql(cls, book_ids, start_date, end_date):
        # 获取产品code
        if not book_ids:
            frame_log.warning('产品code为空')
            return pd.DataFrame()

        book_ids_value = ','.join(["'%s'" % i for i in book_ids])
        sql = f"""with recursive temp as (
            select
                *
            from
                INDIC_DOWN_RELATION p
            where
                PRODUCT_CODE in ({book_ids_value})
                and VALUATION_DATE between '{start_date}' and '{end_date}'
            union all
            select
                t.*
            from
                INDIC_DOWN_RELATION t
            inner join temp t2 on
                t2.SECU_PRODUCT = t.PRODUCT_CODE
            limit 4 ) 
                     select
                VALUATION_DATE,
--                 SECU_CODE,
                INVEST_RATE,
                PRODUCT_CODE
            from
                temp"""
        # 穿透后数据
        df = cls.query(sql)
        if df.empty:
            frame_log.warning('穿透后数据为空')
            return pd.DataFrame()
        rename_dict = {
            'VALUATION_DATE': Dimension.TRADE_DATE,
            'PRODUCT_CODE': Dimension.BOOK_ID,
            # 'SECU_CODE': Dimension.PENETRATE_BOOK_ID,
            'INVEST_RATE': Measure.INVEST_RATE
        }
        df = df.rename(columns=rename_dict)
        df[Dimension.TRADE_DATE] = df[Dimension.TRADE_DATE].apply(date_to_str)
        return df

    @classmethod
    def penetrate_children_apply(cls, group_df):
        """穿透children"""
        row = group_df.iloc[0]
        book_id = row[Dimension.BOOK_ID]
        trade_date = row[Dimension.TRADE_DATE]
        return cls.get_pid_list(book_id, trade_date, parent_ids=tuple(), invest_rates=tuple())

    @classmethod
    def get_penetrate_data(cls, book_ids=None, start_date=None, end_date=None):
        """
        获取穿透后数据
        :param book_ids: 产品code
        :param start_date: 开始日期
        :param end_date:结束日期
        :return:
        """
        cond = {}
        if book_ids:
            cond = {Dimension.BOOK_ID: book_ids}
        # 穿透关系表
        cls.df = cls.get_data(cond=cond, start_date=start_date, end_date=end_date)
        if cls.df.empty:
            frame_log.warning(f'get_penetrate_data获取穿透关系数据为空，book_ids:{book_ids}, start_date:{start_date}, end_date:{end_date}')
            return pd.DataFrame()

        # 穿透
        group_columns = [Dimension.BOOK_ID, Dimension.TRADE_DATE]
        show_columns = [Dimension.TRADE_DATE, Dimension.CHILDREN_BOOK_ID, 'real_invest_rate', 'level1_book_id']
        data = cls.df.groupby(group_columns, group_keys=False).apply(cls.penetrate_children_apply)[show_columns]
        # invest_rate.sum()
        group_columns = ['level1_book_id', Dimension.CHILDREN_BOOK_ID, Dimension.TRADE_DATE]
        data = data.groupby(group_columns, as_index=False)['real_invest_rate'].sum()

        data = data.rename(columns={'real_invest_rate': Measure.INVEST_RATE})

        return data

    @classmethod
    @lru_cache()
    def get_pid_list(cls, book_id, trade_date, parent_ids=tuple(), invest_rates=tuple()):
        """
        递归
        :param book_id: 产品code
        :param trade_date: 日期
        :param parent_ids: 父级产品ids
        :param invest_rate: 占比
        :return:
        """
        if not hasattr(cls, 'df'):
            cls.df = cls.get_data()
        tmp_df = cls.df.query(f"{Dimension.BOOK_ID}==@book_id and {Dimension.TRADE_DATE}==@trade_date")
        if tmp_df.empty:
            return pd.DataFrame()
        res_df = pd.DataFrame()
        dfs = []
        for _index, row in tmp_df.iterrows():
            row_trade_date = row[Dimension.TRADE_DATE]
            row_book_id = row[Dimension.CHILDREN_BOOK_ID]

            new_parent_ids = copy.deepcopy(parent_ids)
            new_invest_rates = copy.deepcopy(invest_rates)
            parent_book_id = row[Dimension.BOOK_ID]
            if parent_book_id in new_parent_ids:
                raise QtException(msg='穿透后数据异常', book_id=book_id, trade_date=trade_date)

            new_parent_ids += (row[Dimension.BOOK_ID],)
            new_invest_rates += (row[Measure.INVEST_RATE],)

            pid_df = cls.get_pid_list(book_id=row_book_id, trade_date=row_trade_date, parent_ids=new_parent_ids,
                                      invest_rates=new_invest_rates, )
            row_dict = row.to_dict()
            row_dict['parent_ids'] = new_parent_ids
            row_dict['rates'] = new_invest_rates
            row_dict['real_invest_rate'] = np.prod(new_invest_rates)
            if not pid_df.empty:
                dfs.append(pid_df)
            dfs.append(pd.DataFrame([row_dict]))

            # res_df = pd.concat([pid_df, pd.DataFrame([row_dict])], ignore_index=True)
            # dfs.append(res_df)
        if dfs:
            res_df = pd.concat(dfs)
            res_df.drop(columns=Measure.INVEST_RATE, inplace=True)
            res_df['level1_book_id'] = book_id
        return res_df


if __name__ == '__main__':
    pd.set_option('display.max_columns', None)
    DownRelationPortfolio.run_etl()
    # res = DownRelationPortfolio.get_penetrate_data(book_ids='demo4')
    res = DownRelationPortfolio.get_pid_list(book_id='demo4',trade_date='20220120')
    print(res)
    print(res.columns)
