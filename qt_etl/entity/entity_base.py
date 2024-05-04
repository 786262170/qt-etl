# vim set fileencoding=utf-8
import asyncio
import copy
import functools
import gc
import os
import shutil
import time
import traceback
from contextvars import ContextVar
from datetime import date, datetime, timedelta
from typing import Optional, Union, Any, List

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds

from qt_common import async_helper, db_manager, utils
from qt_common.error import QtException, QtError
from qt_common.event_handlers import EventHandler, EventRecord
from qt_common.protoc.db.event import EventType, EventStatus
from qt_common.qt_logging import frame_log as logger
from qt_common.utils import date_to_str, month_end, str_to_date, month_beg
from qt_etl.config import settings
from qt_etl.constants import PartitionByDateType
from qt_etl.entity.fields import Dimension
from qt_etl.err_code import EtlError
from qt_etl.utils import deal_date, is_completed


def decode_value(v):
    return v.decode()


class EntityBase:
    """etl数据核心类"""

    main_table = ""
    support_drill_down_cls = set()  # 支持穿透的etl cls集合
    down_flag = ContextVar("is_down", default="1")  # cond过滤条件、是否下钻

    is_concurrent_query = False  # 并发查询
    is_concurrent_save = False  # 并发保存

    partitioned_by_secu_code = False
    partitioned_cols = []  # 按字段分割
    partition_max_rows_per_file = None  # 按数量分割
    partitioned_by_date = None  # 按日期分割（支持，年/月/季度/日）
    partition_max_rows_per_group = 1024 * 1024
    etl_save_path = settings.etl_save_path
    CODE_COLUMN = None
    DATE_COLUMN = None
    USED_TABLE = None
    schema = None

    @classmethod
    def get_partition_dates(cls, start_date: Optional[Union[datetime, date]],
                            end_date: Optional[Union[datetime, date]]):
        m_end = month_end(start_date)
        date_partitions = []
        while m_end < end_date:
            date_partitions.append((start_date, m_end))
            start_date = m_end + timedelta(days=1)
            m_end = month_end(start_date)

        date_partitions.append((start_date, end_date))
        return date_partitions

    @classmethod
    def get_partition_columns(cls):
        schema = cls.schema
        partition_columns = []
        if cls.partitioned_cols:
            for col in cls.partitioned_cols:
                partition_columns.append(schema.field(col))
        return partition_columns

    @classmethod
    def get_partitioning(cls, flavor='hive', partition_columns=None):
        part = None
        if not partition_columns:
            partition_columns = cls.get_partition_columns()
        if partition_columns:
            part = ds.partitioning(
                pa.schema(partition_columns), flavor=flavor)
        return part

    @classmethod
    @utils.timing
    def save_dataset(cls, df, file_path, sign="all"):
        """
        :param df:
        :param file_path:
        :param sign: 标识
        :return:
        """
        schema = copy.deepcopy(cls.schema)
        if schema:
            # 这样定义schema filed字段可以不用和fetch_data返回字段顺序一致
            columns = [i.name for i in schema]
            if len(df):
                df = df[columns]
        t1 = time.time()
        if df.empty:
            logger.warning('df为空')
            return

        if cls.partitioned_by_date == PartitionByDateType.month:
            date_list = df[Dimension.TRADE_DATE].sort_values(ascending=True).unique().tolist()
            beg = month_beg(str_to_date(date_list[0]))
            end = month_end(str_to_date(date_list[-1]))
            try:
                # 文件是不存在的  get_data 时间筛选会报错,所以加上异常捕获
                df_existing = cls.get_data(start_date=beg, end_date=end)
            except Exception as e:
                df_existing = None

            if df_existing is not None and not df_existing.empty:
                df_existing = df_existing[~df_existing[Dimension.TRADE_DATE].isin(df[Dimension.TRADE_DATE].unique())]
                df = pd.concat([df, df_existing])
                df = df.sort_values(Dimension.TRADE_DATE, ascending=False).reset_index(drop=True)

        # 分割字段
        partition_columns = None
        # todo 去除索引
        partition_date_value = None
        # 按日期分割
        if cls.partitioned_by_date:
            partition_columns = cls.get_partition_columns()
            if cls.partitioned_by_date.value not in PartitionByDateType.__members__:
                raise QtException(QtError.E_SUCCESS, msg='暂不支持的参数',
                                  partitioned_by_date=cls.partitioned_by_date)
            if cls.partitioned_by_date == PartitionByDateType.day:
                partition_columns.append(pa.field(Dimension.TRADE_DATE, pa.string()))
            else:
                partition_date_value = df[Dimension.TRADE_DATE].apply(
                    deal_date, args=(cls.partitioned_by_date,)).to_list()
                partition_columns.append(pa.field(cls.partitioned_by_date.value, pa.string()))
                # if not schema:
                #     schema = []
                # schema = schema.append(pa.field(cls.partitioned_by_date.value, pa.string()))

        # 支持某列为空
        table = pa.Table.from_pandas(df, schema=schema)
        if partition_date_value is not None:
            table = table.append_column(cls.partitioned_by_date.value, [partition_date_value])

        part = cls.get_partitioning(partition_columns=partition_columns)
        ds.write_dataset(table, file_path, format='parquet',
                         max_rows_per_file=cls.partition_max_rows_per_file,
                         max_rows_per_group=cls.partition_max_rows_per_group,
                         existing_data_behavior='delete_matching',
                         partitioning=part)
        del df
        gc.collect()
        logger.info(
            "save model:{} sign:{}  ETL data  used time {} output folder:{}",
            cls.__name__, sign, time.time() - t1, file_path)

    @classmethod
    @utils.timing
    def run_etl(cls, secu_codes: Optional[list[str]] = None,
                start_date: Optional[Union[datetime, date]] = date(2021, 1, 1),
                end_date: Optional[Union[datetime, date]] = date.today(),
                is_concurrent_query=False,
                is_concurrent_save=False,
                is_init=False, **kws):
        """
         run etl
        :param secu_codes:
        :param start_date:
        :param end_date:
        :param is_concurrent_query: 是否并发执行（sql 按月分割）
        :param is_concurrent_save: 是否并发save(把并发执行的sql,查询一次save一次)
        :param is_init: 是否初始化（如果初始化删除原来的etl）
        :param kws: 扩展参数
        :return:
        """
        t1 = time.time()
        dfs = []

        # 先查db是否存在event，不存在则注册
        if settings.ENABLED_EVENT:
            event_record = EventRecord(
                model_name=cls.__name__, event_type=EventType.ETL)
            event_model = EventHandler.get_instance(event_record)
            if not event_model:
                event_record.business_date = end_date
                event_record.event_status = EventStatus.START
                EventHandler.register(event_record)
            else:
                if is_completed(event_model, end_date):
                    # 检查event是否已经完成，如果ok不需要重复运行etl生成
                    logger.debug("etl {} has been completed", cls.__name__)
                    if not is_init:
                        # 更新ETL操作，run_etl方法针对event_status为comp时不做限制
                        return
        try:
            if cls.partition_max_rows_per_file and cls.partition_max_rows_per_group > \
                    cls.partition_max_rows_per_file:
                cls.partition_max_rows_per_group = cls.partition_max_rows_per_file
            cls.is_concurrent_query = True if is_concurrent_query else False
            cls.is_concurrent_save = True if is_concurrent_save else False

            # 1.获取etl文件目录
            file_path = cls.get_etl_dir()
            if is_init:
                shutil.rmtree(file_path, ignore_errors=True)  # 删除原来的文件

            # 如果etl文件目录不存在则创建
            if not os.path.exists(file_path):
                os.makedirs(file_path)

            # 2.并发查询
            if cls.is_concurrent_query:
                cls.partitioned_by_date = cls.partitioned_by_date or PartitionByDateType.month
                fetch_data_fns = [
                    functools.partial(
                        cls.fetch_data, secu_codes, start_date=s_date, end_date=e_date)
                    for s_date, e_date in cls.get_partition_dates(start_date=start_date, end_date=end_date)
                ]
                try:
                    dfs = asyncio.run(async_helper.patch_async_run(fetch_data_fns))
                except Exception as e:
                    raise QtException(msg=f"ETL并发查询异常：{e}")
                if dfs:
                    dfs = [_df for _df in dfs if len(_df)]
                if dfs:
                    df = pd.concat(dfs, ignore_index=True)
                else:
                    df = pd.DataFrame()
            else:
                df = cls.fetch_data(secu_codes, start_date, end_date)
            logger.info(
                "Running {} ETL fetch_data total used time {}s, df len:{}",
                cls.__name__, time.time() - t1, len(df))

            # 3.并发保存
            if cls.is_concurrent_save and not df.empty:
                try:
                    asyncio.run(async_helper.patch_async_run(
                        [functools.partial(cls.save_dataset, df=df, file_path=file_path) for df in dfs]
                    ))
                except Exception as e:
                    raise QtException(msg=f"ETL并发保存异常：{e}")
            else:
                try:
                    cls.save_dataset(df, file_path)
                except Exception as e:
                    logger.error(f'save {cls.__name__} model error: {e}')
            logger.info(f'Run ETL model:{cls.__name__} success, len:{len(df)}')

        except Exception as err:
            if settings.ENABLED_EVENT:
                EventHandler.update(
                    event_record, replace_dict=dict(
                        event_status=str(EventStatus.FAILED), business_date=end_date,
                        event_msg=f"Run etl error: f{str(err)}"))
            raise
        else:
            if settings.ENABLED_EVENT:
                EventHandler.update(
                    event_record, replace_dict=dict(
                        event_status=str(EventStatus.COMP), business_date=end_date, event_msg="OK"))
            return len(df)

    @classmethod
    def fetch_data(cls, secu_code: Optional[Union[str, List[str]]] = None,
                   start_date: Optional[Union[datetime, date]] = None,
                   end_date: Optional[Union[datetime, date]] = None, **kwargs):
        raise NotImplemented("")

    @classmethod
    @utils.timing
    def query(cls, sql, session="default", upper_columns=False, as_format=None):
        """sql query
        INFO_开头的资讯表从demo环境获取，业务表走各自当前环境
        """
        tb_name = utils.get_tbname_from_sql(sql)
        if tb_name.upper().startswith("INFO_"):
            if not settings.info_tb_same_app_db:
                session = "info"
        if tb_name.upper().startswith("INDIC_"):
            cls.support_drill_down_cls.add(cls)
        try:
            df = db_manager.pd_read_sql(sql, session, decoder=as_format)
        except Exception as e:
            logger.error(f'query error:{traceback.format_exc()}')
            raise QtException(error=QtError.E_CONNECT, msg=f'query error:{e}')
        if upper_columns:
            df.columns = map(str.upper, df.columns)
        return df

    @classmethod
    def get_etl_file_dir(cls):
        clz_path = cls.mro()
        clz_path.reverse()
        if not cls.partitioned_by_date:
            clz_path = clz_path[2:-1]
        else:
            clz_path = clz_path[2:]
        target_output_folder = cls.etl_save_path
        for p in clz_path:
            target_output_folder = os.path.join(target_output_folder, p.__name__)
        return target_output_folder

    @classmethod
    def get_etl_dir(cls):
        clz_path = cls.mro()
        clz_path.reverse()
        clz_path = clz_path[2:]
        target_output_folder = cls.etl_save_path
        target_output_folder = os.path.join(target_output_folder,
                                            os.path.sep.join([i.__name__ for i in clz_path]))
        return target_output_folder

    @classmethod
    def get_etl_file_name(cls, secu_code: str = None,
                          start_date: Union[date, datetime] = None):

        target_output_folder = cls.get_etl_file_dir()
        if cls.partitioned_by_secu_code:
            return os.path.join(target_output_folder, '{0}.parquet'.format(secu_code))
        elif cls.partitioned_by_date:
            return os.path.join(target_output_folder, '{0}.parquet'.format(start_date.strftime("%Y%m")))
        else:
            return os.path.join(target_output_folder, cls.__name__.lower() + '.parquet')

    @classmethod
    def into_down_flag_para(cls, cond=None):
        """穿透参数注入cond"""
        cond = cond or {}
        # 表字段INDIC开头的ETL支持穿透
        if cls in cls.support_drill_down_cls or cls.main_table.startswith("INDIC_"):
            if Dimension.DOWN_FLAG not in cond:
                # 基于ContexVar线程安全
                cond.update({Dimension.DOWN_FLAG: cls.down_flag.get()})
                logger.info(f"Cond update down_flag params:{cond}")

    @classmethod
    def get_data(cls, secu_codes: Optional[list[str]] = None,
                 start_date: Optional[Union[date, datetime]] = None,
                 end_date: Optional[Union[date, datetime]] = None,
                 cond: Optional[dict[Dimension, Union[list, str]]] = None,
                 columns: Optional[list] = None,
                 decoder: Optional[Any] = None):
        filters = []
        cond = cond or {}
        if start_date:
            start_date = date_to_str(start_date)
            filters.append(f"(ds.field(Dimension.TRADE_DATE) >= '{start_date}')")
        if end_date:
            end_date = date_to_str(end_date)
            filters.append(f"(ds.field(Dimension.TRADE_DATE) <= '{end_date}')")

        if secu_codes:
            cond.update({Dimension.INSTRUMENT_CODE: secu_codes})
        # 下钻参数注入cond
        # cls.into_down_flag_para(  cond)
        # for trip in [None, '', {}, []]:
        #     utils.dict_trip(cond, trip)
        if cond:
            for d, l in cond.items():
                if isinstance(l, list):
                    filters.append(f"(ds.field('{d}').isin({l}))")
                else:
                    filters.append(f"(ds.field('{d}') == '{l}')")
        if not filters:
            filters = None
        if columns is not None:
            if any([start_date, end_date]):
                columns.append(Dimension.TRADE_DATE)
            if cond is not None:
                columns = columns + list(cond.keys())
            columns = list(set(columns))

        start_time = time.time()
        file_path = cls.get_etl_dir()

        # 未生成etl时提示异常
        if not os.path.exists(file_path):
            raise QtException(
                EtlError.E_NOT_EXIST, user_msg=(cls.__name__, cls.__doc__, file_path))

        dataset = ds.dataset(file_path, schema=cls.schema, format='parquet',
                             partitioning=cls.get_partitioning())
        # dataset columns
        dataset_columns = dataset.schema.names
        if columns and Dimension.TRADE_DATE in columns and Dimension.TRADE_DATE not in dataset_columns:
            columns.remove(Dimension.TRADE_DATE)

        # 如果dataset没有trade_date字段，去掉trade_date过滤条件
        if filters and Dimension.TRADE_DATE not in dataset_columns:
            new_filters = []
            for _filter in filters:
                if 'ds.field(Dimension.TRADE_DATE)' in _filter:
                    continue
                else:
                    new_filters.append(_filter)
            filters = new_filters

        try:
            if filters:
                table = dataset.to_table(columns=columns, filter=eval(' & '.join(filters)))
            else:
                table = dataset.to_table(columns=columns)
        except pa.lib.ArrowInvalid as e:
            raise QtException(QtError.E_SUCCESS, msg=f'请检查etl文件，model:{cls.__name__}，{e.args}')
        except Exception as e:
            raise QtException(QtError.E_SUCCESS, msg=f'dataset to table error:{e}')

        df = table.to_pandas().sort_index()
        logger.info('Loading all {} to cache from parquet {} used time:{}'.format(cls.__name__, file_path,
                                                                                  time.time() - start_time))

        if Dimension.TRADE_DATE in df.columns:
            df = df.sort_values(Dimension.TRADE_DATE, ascending=False)

        # data decoder 格式化处理
        if decoder:
            df = decoder(df)

        logger.info(f"获取{cls.__name__}数据: start_date={start_date} end_date={end_date} cond:{cond}")
        return df

    @classmethod
    def get_schema_df(cls, *args, **kwargs):

        schema = cls.schema
        schema_metadata = schema.metadata
        description_df = None
        if schema_metadata:
            name_list = map(decode_value, schema_metadata.keys())
            description_list = map(decode_value, schema_metadata.values())
            description_df = pd.DataFrame({"name": name_list, "description": description_list})

        data = []
        for column in schema:
            column_info = {}
            column_info['name'] = column.name
            column_info['type'] = column.type
            column_info['nullable'] = column.nullable
            metadata = column.metadata
            if not metadata:
                continue
            for k, v in metadata.items():
                column_info[k.decode()] = v.decode()
            data.append(column_info)

        df = pd.DataFrame(data)
        df['model'] = cls.__name__
        if len(description_df):
            df = df.merge(description_df, how='left')

        return df

    # --- entity format ---
    @classmethod
    def rename(cls, df: pd.DataFrame):
        """etl field rename"""
        if df.empty:
            return df
        if not any([cls.schema, cls.etl_rename_dict]):
            return df

        if cls.schema:
            rename_dict = {}
            for field in cls.schema:
                table_field = field.metadata.get(b"table_field").decode("utf8")
                if table_field:
                    rename_dict[table_field] = field.name
            return df.rename(columns=rename_dict)
        elif cls.etl_rename_dict:
            return df.rename(columns=cls.etl_rename_dict)
        else:
            return df

    etl_rename_dict: dict = {}
