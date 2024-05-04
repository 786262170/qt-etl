# vim set fileencoding=utf-8

import json
from datetime import date, datetime
from functools import lru_cache
from typing import Optional, Union

import pandas as pd
import pyarrow as pa

from qt_common.error import QtException, QtError
from qt_common.utils import date_to_str
from qt_etl.entity.fields import Dimension
from qt_etl.entity.market_data.market_data import MarketData

__all__ = ["IndustryClassificationMktData"]


class IndustryClassificationMktData(MarketData):
    """行业分类市场数据"""
    code_source_table = {
        "INFO_PARA_INFO": "PARA_CODE",
        "INFO_PARTY_INDUSTRY": "PARTY_CODE",
    }
    schema = pa.schema([
        pa.field(Dimension.ISSUER_CODE, pa.string(),
                 metadata={b'source_table': json.dumps(code_source_table).encode()}),
        pa.field(Dimension.TRADE_DATE, pa.string(),
                 metadata={b'table_field': b'START_DATE', b'table_name': b'INFO_PARTY_INDUSTRY'}),
        pa.field(Dimension.INDUSTRY_CODE, pa.string(),
                 metadata={b'table_field': b'INDU_SYS_CODE', b'table_name': b'INFO_PARTY_INDUSTRY'}),
        pa.field(Dimension.SECTOR_CODE, pa.string(),
                 metadata={b'table_field': b'INDU_CODE_1ST', b'table_name': b'INFO_PARTY_INDUSTRY'}),
        pa.field(Dimension.SECTOR_NAME, pa.string(),
                 metadata={b'table_field': b'INDU_NAME_1ST', b'table_name': b'INFO_PARTY_INDUSTRY'}),
        pa.field(Dimension.INDUSTRY_NAME, pa.string(),
                 metadata={b'table_field': b'PARA_NAME', b'table_name': b'INFO_PARA_INFO'}),
    ],
        metadata={
            Dimension.ISSUER_CODE: '主体编码',
            Dimension.TRADE_DATE: '变动起始日',
            Dimension.INDUSTRY_CODE: '行业分类体系内部编码',
            Dimension.SECTOR_CODE: '一级行业分类代码',
            Dimension.SECTOR_NAME: '一级行业分类名称',
            Dimension.INDUSTRY_NAME: '常量名称',
        }
    )
    partitioned_by_date = False

    """股票市场波动数据"""

    # 一级行业分类代码
    # 一级行业分类名称
    # 行业分类体系内部编码

    # 参数表中对应股票行业分类的记录
    PARA_OBJ = '020601'

    #
    # INSTRUMENT_NAME = '中信行业'

    @classmethod
    def fetch_data(cls,
                   secu_code: str = None,
                   start_date: Optional[Union[datetime, date]] = None,
                   end_date: Optional[Union[datetime, date]] = None):
        # 获取para_code
        sql = f"""SELECT
                    PARA_CODE,
                    PARA_NAME
                FROM
                    INFO_PARA_INFO
                WHERE
                    PARA_OBJ = '{cls.PARA_OBJ}'"""
        df_industry_info = cls.query(sql)
        df_industry_info = df_industry_info.rename(columns={
            'PARA_CODE': Dimension.INDUSTRY_CODE,
            'PARA_NAME': Dimension.INDUSTRY_NAME
        })
        para_code_list = df_industry_info[Dimension.INDUSTRY_CODE].to_list()
        if not para_code_list:
            raise QtException(QtError.E_NOT_EXIST, f'para_code为空')
        code_value = ','.join(["'%s'" % item for item in para_code_list])

        sql = f"""SELECT
            PARTY_CODE,
            START_DATE,
            INDU_SYS_CODE,
            INDU_CODE_1ST,
            INDU_NAME_1ST
        FROM
            INFO_PARTY_INDUSTRY
        WHERE INDU_SYS_CODE IN ({code_value})"""
        # AND START_DATE  > TRUNC(SYSDATE)-(INTERVAL '2' YEAR)
        df = cls.query(sql)
        if not df.empty:
            df = df.rename(columns=cls.etl_rename_dict)
            df[Dimension.TRADE_DATE] = df[Dimension.TRADE_DATE].apply(date_to_str)
            df = df.merge(df_industry_info, how='left', on=Dimension.INDUSTRY_CODE)
            df = df.reset_index(drop=True)

        return df

    etl_rename_dict = {
        'PARTY_CODE': Dimension.ISSUER_CODE,
        'START_DATE': Dimension.TRADE_DATE,
        'INDU_SYS_CODE': Dimension.INDUSTRY_CODE,
        'INDU_CODE_1ST': Dimension.SECTOR_CODE,
        'INDU_NAME_1ST': Dimension.SECTOR_NAME,
    }

    @classmethod
    def append_stock_sector(cls, df_stock, industry_code):
        # 根据df_stock.issuer_code, and df_stock.trade_date >= df_ind.start_date过滤df_ind，然后选择最后一个（日期最近的）
        try:
            df_ind = cls.get_all_industry_data()
            df_ind = df_ind[(df_ind[Dimension.INDUSTRY_CODE] == industry_code) & (
                df_ind[Dimension.ISSUER_CODE].isin(df_stock[Dimension.ISSUER_CODE].tolist()))]
            dfs = []
            for d, g in df_stock.groupby(Dimension.TRADE_DATE):
                df_ind_t = df_ind[df_ind[Dimension.TRADE_DATE] <= d]
                df_ind_t = df_ind_t.drop_duplicates(subset=[Dimension.ISSUER_CODE], keep='last')
                df_ind_t = df_ind_t[[Dimension.ISSUER_CODE, Dimension.SECTOR_CODE]]
                dfs.append(g.merge(df_ind_t, how='left', on=[Dimension.ISSUER_CODE]))

            return pd.concat(dfs)
        except Exception as e:
            a = e
            return pd.DataFrame()

    @classmethod
    @lru_cache(1)
    def get_all_industry_data(cls):
        return cls.get_data()

    @classmethod
    def get_indu_name_1st(cls, codes=[], response_type='df', industry_class_code=None):
        cond = {}
        if codes:
            if not isinstance(codes, list):
                codes = list(codes)
            cond[Dimension.SECTOR_CODE] = codes
        if industry_class_code:
            if not isinstance(industry_class_code, list):
                industry_class_code = [industry_class_code]
            cond[Dimension.INDUSTRY_CODE] = industry_class_code
        columns = [Dimension.SECTOR_CODE, Dimension.SECTOR_NAME]
        df = cls.get_data(cond=cond, columns=columns)
        data = {}
        if len(df):
            df.drop_duplicates(inplace=True)
            if response_type == 'dict':
                data = {}
                for index, row in df.iterrows():
                    data[row[Dimension.SECTOR_CODE]] = row[Dimension.SECTOR_NAME]
                return data
        if response_type == 'dict':
            return data
        else:
            return df


if __name__ == '__main__':
    IndustryClassificationMktData.run_etl(start_date=datetime(2020, 12, 10),
                                          end_date=date(2021, 11, 10))
    df = classification_dict = IndustryClassificationMktData.get_indu_name_1st(response_type='dict',
                                                                               industry_class_code=['075'])
    print(df)
