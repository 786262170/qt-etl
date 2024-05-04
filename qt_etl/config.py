# vim set fileencoding=utf-8
"""etl config"""
import os

from pydantic import validator

from qt_depends.config import CommonConfig, get_settings

APP_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STATIC_PATH = os.path.join(APP_PATH, "static")


class EtlSettings(CommonConfig):
    etl_save_path: str = None  # 数据存放的目录
    info_tb_same_app_db: bool = False  # 基础信息数据是否跟随主业务表, 为True时, info表统一从demo环境读取
    enabled_flow_log: bool = False  # etl执行是否注册prefect任务, 当通过prefect平台调度时开启
    enabled_event: bool = False  # 是否启用event

    @validator("etl_save_path", pre=False)
    def validate_etl_save_path(cls, etl_save_path, values):
        if not etl_save_path:
            run_env = values['run_env']
            etl_save_path = os.path.join(STATIC_PATH, f"qlib-{run_env}")
        return etl_save_path


settings: EtlSettings = get_settings(EtlSettings)


def get_config(section, *keys, **kwargs):
    return settings.conf.get(section, *keys, **kwargs)
