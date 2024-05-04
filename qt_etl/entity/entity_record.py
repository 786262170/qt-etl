# vim set fileencoding=utf-8
# coding=utf-8
"""etl 调度记录"""
import copy
import datetime
import enum
import functools
import uuid
from typing import Union, List, Any, Tuple

import pandas
import pandas as pd
from pydantic import BaseModel

from qt_common.error import QtError, QtException
from qt_common.qt_logging import frame_log
from qt_etl.entity.entity_base import EntityBase
from qt_etl.entity.factor.factor import Factor
from qt_etl.entity.instruments.instrument import Instrument
from qt_etl.entity.market_data.market_data import MarketData
from qt_etl.entity.portfolio.portfolio import Portfolio
from qt_etl.entity.trade_info.trade_info import TradeInfo


class EtlCategory(enum.Enum):
    """etl 数据分类"""
    Instrument = Instrument.__name__
    MarketData = MarketData.__name__
    Portfolio = Portfolio.__name__
    Trade = TradeInfo.__name__
    Factor = Factor.__name__


class EtlModels(BaseModel):
    category_code: str = None
    category_name: str = None
    category_cls: Any = None
    model_code: str = None
    model_name: str = None
    model_cls: Any = None


class EtlActionEnum(enum.Enum):
    read = "get_data"
    write = "run_etl"


def export_model_schema(model):
    main_table = model.main_table if hasattr(model, "main_table") else ''
    items = []
    schema = model.schema
    if schema:
        model_metadata = schema.metadata or {}
        if model_metadata:
            model_metadata = {
                k.decode()
                if isinstance(k, bytes)
                else k: v.decode()
                if isinstance(v, bytes)
                else v
                for k, v in model_metadata.items()
            }

        for pa_field in schema:
            table_fields = None
            item = {'field_name': pa_field.name, 'field_type': pa_field.type}
            metadata = pa_field.metadata
            if metadata:
                metadata = {k.decode(): v.decode() for k, v in metadata.items()}
                table_field = metadata.get('table_field')
                table_name = metadata.get('table_name', main_table)

                if table_field and table_field.startswith('['):
                    try:
                        table_fields = eval(table_field)
                    except Exception as e:
                        print(f'table_field定义有误， table_field:{table_field}')
                source_table = metadata.get('source_table', {})
                if source_table:
                    try:
                        source_table = eval(source_table)
                    except Exception as e:
                        print(f'source_table定义有误， source_table:{source_table}')
                description = model_metadata.get(pa_field.name)
                item['field_description'] = description

                if source_table:
                    for key, val in source_table.items():
                        sub_item = {"table_name": key, "table_field": val}
                        sub_item.update(item)
                        items.append(sub_item)
                elif table_fields:
                    for _field in table_fields:
                        cope_item = copy.deepcopy(item)
                        cope_item['table_name'] = table_name
                        cope_item['table_field'] = _field
                        items.append(cope_item)
                elif table_field:
                    item['table_name'] = table_name
                    item['table_field'] = table_field
                    items.append(item)
                else:
                    items.append(item)

    return pd.DataFrame(items)


def get_subclasses(cls):
    """获取当前子类"""
    return [subcls for subcls in cls.__subclasses__()]


def get_cls_attribute(cls):
    """回去类属性
    :return (类名称, 类注释, 类实体) example:("BondInfo","债券基本信息", BondInfo)
    """
    return cls.__name__, cls.__doc__, cls


# 注册etl分类和具体模型映射
ENTITY_MAP = {}
ENTITY_DF = pandas.DataFrame(
    columns=[
        "category_code",
        "category_name",
        "category_cls",
        "model_code",
        "model_name",
        "model_cls",
    ])


def build_entity_df():
    dataframes = []
    for subcls in get_subclasses(EntityBase):
        for modelcls in get_subclasses(subcls):
            record = EtlModels()
            (
                record.category_code,
                record.category_name,
                record.category_cls,
            ) = get_cls_attribute(subcls)
            (
                record.model_code,
                record.model_name,
                record.model_cls,
            ) = get_cls_attribute(modelcls)
            dataframes.append(record.dict())
    global ENTITY_DF
    ENTITY_DF = pandas.concat([pandas.DataFrame(dataframes)], ignore_index=False, sort=True)


build_entity_df()


def get_schema_info(entity=None):
    """
    根据对应分类获取对应分类下schema详情
    :param entity: EntityBase实体类
    :return:默认返回所有EntityBase视图类schema dataframe
    """
    columns = [
        "category_code",
        "category_name",
        "model_code",
        'model_name',
        'field_name',
        'field_type',
        "field_description",
        'table_name',
        'table_field',
        "table_description",
    ]
    if entity is None:
        entity = EntityBase
    model_schemas_df = []
    for entity_category_cls in get_subclasses(entity):
        frame_log.info("etl category: {} schema generate", str(entity_category_cls))
        for entity_model_cls in get_subclasses(entity_category_cls):
            frame_log.info("etl model: {} schema generate", str(entity_model_cls))
            schema_df = export_model_schema(entity_model_cls)
            if not schema_df.empty:
                schema_df['model_code'], schema_df['model_name'], _ = get_cls_attribute(
                    entity_model_cls)
                schema_df['category_code'], schema_df['category_name'], _ = get_cls_attribute(
                    entity_category_cls)
                schema_df['table_description'] = "-"
                schema_df = schema_df[columns]
                schema_df.fillna(value="-", inplace=True)
                schema_df = schema_df.astype(str)
                model_schemas_df.append(schema_df)
    res_df = pd.concat(model_schemas_df)
    res_df = res_df.sort_values(by=["category_name", "model_name", "table_name"])
    res_df = res_df.astype(str)
    # res_df.columns = map(str.upper, res_df.columns)
    return res_df


def get_etl_action_callable(
        category_code: str = None, model_code: str = None,
        action: EtlActionEnum = EtlActionEnum.read, **kwargs) -> \
        Union[List[Tuple[str, functools.partial]],
              Tuple[str, functools.partial], None]:
    """获取etl操作函数
    :param category_code: 模型分类代码， "ALL"时获取所有模型
    :param model_code: 模型代码
    :param action: 函数操作，默认EtlActionEnum.read
    :param kwargs: 关键字参数
    :return:run_etl or get_data
    """
    df = copy.deepcopy(ENTITY_DF)
    if category_code == "ALL":
        df = df.loc[df.category_code.isin(EtlCategory.__members__)]
    elif category_code in EtlCategory.__members__:
        df = df.loc[df.category_code == category_code]
    if model_code:
        df = df.loc[df.model_code == model_code]
    if df.empty:
        raise QtException(QtError.E_NOT_EXIST, "etl model not found")
    if len(df) > 1:
        return [get_etl_action_callable(
            row.category_code, row.model_code, action, **kwargs) for _, row in
            df.iterrows()]
    else:
        model_cls = df.model_cls.iloc[0]
        if not hasattr(model_cls, action.value):
            raise RuntimeError("模型未继承EntityBase, 请检查")
        action_callable = functools.partial(
            getattr(model_cls, action.value), **kwargs)
        return model_cls.__name__, action_callable


def export_schema_file(filename=None):
    """导出csv文件到本地"""
    filename = filename or f'{uuid.uuid1()}-{datetime.date.today()}.csv'
    res_df = get_schema_info()
    res_df.to_csv(filename, index=False, encoding='utf_8_sig')
    return filename


def insert_etl_data_by_df(df_data):
    from qt_common.db_manager import pd_to_sql
    pd_to_sql(df_data, tb_name="INFO_ETL_FIELD_MAP", session="qt_quant")


if __name__ == '__main__':
    print(get_etl_action_callable(category_code="ALL"))
    print(len(get_etl_action_callable(category_code="ALL")))
