# vim set fileencoding=utf-8
from datetime import date, datetime
from typing import Optional, Union

import pandas as pd
import pyarrow as pa

from qt_common.db_manager import pd_read_sql
from qt_common.qt_logging import frame_log
from qt_etl.entity.asset.asset_base import Asset
from qt_etl.entity.fields import Dimension

__all__ = ["AssetClassification"]


class AssetClassification(Asset):
    """资产分类"""
    main_table = 'PLATO_CS_PRD_STR_CLS'
    schema = pa.schema([
        pa.field(Dimension.CLS_TYPE, pa.string(), metadata={b'table_field': b'CLS_TYPE_CODE'}),
        pa.field(Dimension.CLS_CODE, pa.string(), metadata={b'table_field': b'CLS_CODE'}),
        pa.field(Dimension.CLS_NAME, pa.string(), metadata={b'table_field': b'CLS_NAME'})
    ],
        metadata={
            Dimension.CLS_TYPE: '所属分类方式',
            Dimension.CLS_CODE: '分类编码',
            Dimension.CLS_NAME: '分类名称'
        }
    )

    etl_rename_dict = {
        "CLS_TYPE_CODE": Dimension.CLS_TYPE,
        "CLS_TYPE": Dimension.CLS_TYPE,
        "CLS_CODE": Dimension.CLS_CODE,
        "CLS_NAME": Dimension.CLS_NAME
    }

    @classmethod
    def fetch_data(cls,
                   secu_code: str = None,
                   start_date: Optional[Union[datetime, date]] = None,
                   end_date: Optional[Union[datetime, date]] = None):
        dfs = []
        # 系统资产分类
        sys_asset_df = cls.get_sys_asset_classification()
        if not sys_asset_df.empty:
            dfs.append(sys_asset_df.drop_duplicates())
        # 用户自定义资产分类
        user_asset_df = cls.get_user_defined_classification()
        if not user_asset_df.empty:
            dfs.append(user_asset_df.drop_duplicates())
        if dfs:
            df = pd.concat([sys_asset_df, user_asset_df])
            df = df.reset_index(drop=True)
        else:
            df = user_asset_df
        return df

    @classmethod
    def get_user_defined_classification(cls,
                                        secu_code: str = None,
                                        start_date: Optional[Union[datetime, date]] = None,
                                        end_date: Optional[Union[datetime, date]] = None):
        # 用户资产分类
        sql = f"""SELECT
            PLATO_CS_PRD_ASSET_CLS_UD.CLS_TYPE_CODE,
            PLATO_CS_PRD_ASSET_CLS_UD.CLS_CODE,
	        PLATO_CS_PRD_ASSET_CLS_UD.CLS_NAME
        FROM
            PLATO_CS_PRD_ASSET_CLS_UD
        INNER JOIN PLATO_CS_PRD_ASSET_CLS_UD_TYPE
        ON
            PLATO_CS_PRD_ASSET_CLS_UD.CLS_TYPE_CODE = PLATO_CS_PRD_ASSET_CLS_UD_TYPE.CLS_TYPE_CODE"""
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
            PLATO_CS_PRD_ASSET_CLS_SYS.CLS_TYPE,
            PLATO_CS_PRD_ASSET_CLS_SYS.CLS_CODE,
	        PLATO_CS_PRD_ASSET_CLS_SYS.CLS_NAME
        FROM
            PLATO_CS_PRD_ASSET_CLS_SYS
        INNER JOIN PLATO_CS_PRD_ASSET_CLS_SYS_TYPE
        ON PLATO_CS_PRD_ASSET_CLS_SYS.CLS_TYPE = PLATO_CS_PRD_ASSET_CLS_SYS_TYPE.CLS_TYPE_CODE
       """
        try:
            df = pd_read_sql(sql, session="plato_appl")
        except Exception as e:
            frame_log.error(e)
        if len(df):
            df = df.rename(columns=cls.etl_rename_dict)
        return df


if __name__ == '__main__':
    AssetClassification.run_etl(is_init=True)
