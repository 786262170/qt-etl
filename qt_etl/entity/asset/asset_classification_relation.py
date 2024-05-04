# vim set fileencoding=utf-8
from datetime import date, datetime
from typing import Optional, Union

import pandas as pd
import pyarrow as pa

from qt_common.db_manager import pd_read_sql
from qt_common.qt_logging import frame_log
from qt_etl.entity.asset.asset_base import Asset
from qt_etl.entity.fields import Dimension

__all__ = ["AssetClassificationRelation"]


class AssetClassificationRelation(Asset):
    """个券自定义资产分类"""
    main_table = 'PLATO_CS_PRD_STR_CLS'
    schema = pa.schema([
        pa.field(Dimension.INSTRUMENT_CODE, pa.string(), metadata={b'table_field': b'SECU_CODE'}),
        pa.field(Dimension.SYMBOL, pa.string(), metadata={b'table_field': b'SYMBOL'}),
        pa.field(Dimension.INSTRUMENT_NAME, pa.string(), metadata={b'table_field': b'SECU_NAME'}),
        pa.field(Dimension.CLS_TYPE, pa.string(), metadata={b'table_field': b'CLS_TYPE_CODE'}),
        pa.field(Dimension.CLS_CODE, pa.string(), metadata={b'table_field': b'CLS_CODE'}),
        pa.field(Dimension.CLS_NAME, pa.string(), metadata={b'table_field': b'CLS_NAME'})
    ],
        metadata={
            Dimension.INSTRUMENT_CODE: '个券内码',
            Dimension.SYMBOL: '个券编码',
            Dimension.INSTRUMENT_NAME: '个券名称',
            Dimension.CLS_TYPE: '所属分类方式',
            Dimension.CLS_CODE: '分类编码',
            Dimension.CLS_NAME: '分类名称'
        }
    )

    etl_rename_dict = {
        "SECU_CODE": Dimension.INSTRUMENT_CODE,
        "SYMBOL": Dimension.SYMBOL,
        "SECU_NAME": Dimension.INSTRUMENT_NAME,
        "CLS_TYPE_CODE": Dimension.CLS_TYPE,
        "CLS_TYPE": Dimension.CLS_TYPE,
        "CLS_CODE": Dimension.CLS_CODE,
        "CLS_NAME": Dimension.CLS_NAME,

    }

    @classmethod
    def fetch_data(cls,
                   secu_code: str = None,
                   start_date: Optional[Union[datetime, date]] = None,
                   end_date: Optional[Union[datetime, date]] = None):
        # 系统资产分类
        sys_asset_df = cls.get_sys_asset_classification()
        # 用户自定义资产分类
        user_asset_df = cls.get_user_defined_classification()
        if len(sys_asset_df) and len(user_asset_df):
            df = pd.concat([sys_asset_df, user_asset_df])
            df = df.reset_index(drop=True)
        elif len(sys_asset_df):
            df = sys_asset_df
        else:
            df = user_asset_df

        return df

    @classmethod
    def get_user_defined_classification(cls,
                                        secu_code: str = None,
                                        start_date: Optional[Union[datetime, date]] = None,
                                        end_date: Optional[Union[datetime, date]] = None):
        # 系统资产分类
        sql = f"""SELECT
            PLATO_CS_PRD_STR_CLS.SECU_CODE,
            PLATO_CS_PRD_STR_CLS.SYMBOL,
            PLATO_CS_PRD_STR_CLS.SECU_NAME,
            PLATO_CS_PRD_ASSET_CLS_UD.CLS_TYPE_CODE,
            PLATO_CS_PRD_ASSET_CLS_UD.CLS_CODE,
            PLATO_CS_PRD_ASSET_CLS_UD.CLS_NAME
        FROM
            PLATO_CS_PRD_STR_CLS
        INNER JOIN PLATO_CS_PRD_ASSET_CLS_UD
        ON
            PLATO_CS_PRD_ASSET_CLS_UD.CLS_CODE = PLATO_CS_PRD_STR_CLS.CLS_CODE"""
        try:
            df = pd_read_sql(sql, session="plato_appl")
        except Exception as e:
            frame_log.error(e)
        if len(df):
            df = df.rename(columns=cls.etl_rename_dict)

        return df

    @classmethod
    def get_sys_asset_classification(cls,
                                     secu_code: str = None,
                                     start_date: Optional[Union[datetime, date]] = None,
                                     end_date: Optional[Union[datetime, date]] = None):
        # 系统资产分类
        sql = f"""SELECT
            PLATO_CS_PRD_STR_CLS.SECU_CODE,
            PLATO_CS_PRD_STR_CLS.SYMBOL,
            PLATO_CS_PRD_STR_CLS.SECU_NAME,
            PLATO_CS_PRD_ASSET_CLS_SYS.CLS_TYPE,
            PLATO_CS_PRD_STR_CLS.CLS_CODE,
            PLATO_CS_PRD_STR_CLS.CLS_NAME
        FROM
            PLATO_CS_PRD_STR_CLS
        INNER JOIN PLATO_CS_PRD_ASSET_CLS_SYS
        ON
            PLATO_CS_PRD_ASSET_CLS_SYS.CLS_CODE = PLATO_CS_PRD_STR_CLS.CLS_CODE"""
        try:
            df = pd_read_sql(sql, session="plato_appl")
        except Exception as e:
            frame_log.error(e)
        if len(df):
            df = df.rename(columns=cls.etl_rename_dict)
        return df


if __name__ == '__main__':
    AssetClassificationRelation.run_etl(is_init=True)
