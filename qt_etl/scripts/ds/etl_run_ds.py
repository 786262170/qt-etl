# vim set fileencoding=utf-8
# coding=utf-8
"""供dolphin scheduler调度平台使用, 所有代码均使用python3内置库，如ds不支持兼容的语法，通知作者修改

阻塞式调用异步接口，可配置轮询参数--retry_time， 默认为每s查询一次， 直到模型cache有值为止，视为ETL生成成功

:Author: changye.yang@iquantex.com
:Date: 2023/02/19
# 帮助信息
python etl_run_ds.py --help
# 例子
1. 跑单个模型
python etl_run_ds.py -m CombPosition
2. 跑单个大类
python etl_run_ds.py -c Portfolio
"""

import argparse
import enum
import json
import logging
import time
from collections import namedtuple
from datetime import date
from urllib import parse
from urllib.request import Request, urlopen

_DEFAULT_REQUEST_URI = "http://127.0.0.1:5000"  # 默认请求URL
_DEFAULT_REQUEST_TIMEOUT = 300  # 默认300s接口响应时间
_DEFAULT_RUN_ENV = "local"  # 默认运行环境


class SysEnv(enum.Enum):
    local = "local"
    dev = "dev"
    sit = "sit"
    prd = "prd"
    demo = "demo"

    def __str__(self):
        return self.value


def get_loggerr():
    """日志格式化"""
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    ch = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s - %(module)s - %(funcName)s - %(filename)s[line:%(lineno)d] - %(levelname)s - %(message)s"
    )
    ch.setFormatter(formatter)
    # fh.setFormatter(formatter)
    logger.addHandler(ch)
    return logger


frame_log = get_loggerr()

Error = namedtuple("Error", ["errno", "msg", "user_msg"])


class ErrorMeta(type):
    """异常元类

    动态控制类的生成格式、字符描述信息
    """

    def __new__(mcs, clsname, bases, attrs):
        for key, value in attrs.items():
            if key.startswith("E_") and isinstance(value, tuple):
                if len(value) < 3:
                    attrs[key] = Error(*value, user_msg="系统错误")
                else:
                    attrs[key] = Error(*value)
        setattr(Error, '__str__', lambda x: "<%d: %s>" % (x.errno, x.msg))
        return super(ErrorMeta, mcs).__new__(mcs, clsname, bases, attrs)


class QtError(metaclass=ErrorMeta):  # pylint: disable=too-few-public-methods
    """ 异常状态码描述 """
    # Base Error
    E_BASEERROR = (1000, 'base error')

    # other system error
    E_OTHER_BASE = (1500, 'other system error')
    E_CONNECT = (1501, 'connect other failed')


class QtException(Exception):
    """异常元类"""
    def __init__(self, error=QtError.E_BASEERROR, msg=None, **kwargs):
        # pylint: disable=no-member
        self.error = error
        self.msg = msg or error.msg
        self.kwargs = kwargs
        self.user_msg = kwargs.get('user_msg', error.user_msg)
        super(QtException, self).__init__(self.msg)

    def __reduce_ex__(self, proto=None):
        return type(self), (self.error, self.msg), self.__dict__

    def __str__(self):
        return "%s(%s)" % (self.msg, self.error.errno)  # pylint: disable=no-member

    def __repr__(self):
        """ Messages for user """
        return self.user_msg


def dict_trip(data, trip=None):
    """对字典中为'值'为「trip」的字段进行清理"""
    for key in list(data.keys()):
        if data[key] == trip:
            data.pop(key)
    return data


def build_cgi_request(url, method, data, headers, prepare, **para):
    """封装urlib.Request类"""
    para = dict_trip(para)
    headers = headers or {}
    if para:
        url += '?' + parse.urlencode(para)
    if isinstance(data, dict):
        data = json.dumps(data)
    if isinstance(data, str):
        data = data.encode("utf-8")
    req = Request(url, data, headers=headers)
    if method in ["PUT", "POST", "DELETE", "OPTION"]:
        req.get_method = lambda: method
    if prepare:
        prepare(req)
    frame_log.info("cgi request:%s|method:%s|headers:%s|body:%s",
                   req.get_full_url(), req.get_method(),
                   req.headers, str(req.data))
    return req


def cgi_request_sync(url, method=None, data=None, prepare=None, decoder=None,
                     # pylint: disable=too-many-arguments
                     headers=None, timeout=None, resp_header=None, **para):
    """同步cgi请求封装

    基于内置库urlib二次封装，增加了日志、异常和请求返回头信息处理
    """
    req = build_cgi_request(url, method, data, headers, prepare, **para)
    if timeout is None:
        timeout = _DEFAULT_REQUEST_TIMEOUT
    try:
        resp = urlopen(req, timeout=timeout)
    except TimeoutError:
        frame_log.exception("request timed out")
        raise QtException(QtError.E_CONNECT, "request timed out")
    except Exception as err:
        frame_log.exception("open failed:%s", err)
        raise QtException(QtError.E_CONNECT, f"{para}:{err}")
    if resp.getcode() != 200:
        frame_log.error("open failed:%s", resp.getcode())
        raise QtException(QtError.E_OTHER_BASE, f"{url}:{resp.getcode()}")
    content = resp.read()
    if isinstance(content, bytes):
        content = content.decode("utf-8")
    frame_log.info("resp:%s", content)
    if decoder:
        try:
            content = decoder(content)
        except QtException:
            raise
        except Exception as err:
            frame_log.exception("decode error")
            raise QtException(QtError.E_OTHER_BASE,
                              f"{url}: decoder resp error.({err})")
    if resp_header is not None:
        frame_log.info("headers:%s", resp.headers)
        resp_header.update(resp.headers)
    return content


class Api:
    """Api接口工具类"""
    RUN_ENV = _DEFAULT_RUN_ENV  # 默认运行环境：[local|dev|sit|demo|prd]
    REQUEST_URL = None  # 默认domain
    REQUEST_TIMEOUT = _DEFAULT_REQUEST_TIMEOUT  # 默认接口请求时间，如超时会自动停止请求

    @classmethod
    def get_domain(cls, run_env=None):
        """获取请求url域名信息"""
        if cls.REQUEST_URL:
            return cls.REQUEST_URL
        run_env = run_env or cls.RUN_ENV
        if run_env not in SysEnv.__members__:
            raise TypeError("run_env must be [local|sit|dev|demo|prd]")
        if run_env == str(SysEnv.local):
            return _DEFAULT_REQUEST_URI
        domain = "https://{}.plato.iquantex.com".format(run_env)
        return domain

    @staticmethod
    def decoder_resp(content):
        """返回结构解析"""
        content = json.loads(content)
        code, msg = content.get("code"), content.get("msg"),
        err_code, data = content.get("errCode"), content.get("data"),
        if code != 200:
            raise QtException(msg=f"接口错误-{(err_code, msg)}")
        result = data.get("result", {}) or {}
        if not result:
            raise QtException(msg=f"接口错误-{(err_code, msg)}")
        return result

    # --- 以下为定义接口 ---

    @classmethod
    def run_task(cls, category_code=None, model_code=None, secu_codes=None,
                 start_date=None, end_date=date.today().strftime("%Y-%m-%d"),
                 is_concurrent_query=False,
                 is_concurrent_save=False,
                 is_init=False):
        """etl运行后台任务"""
        rest = "/qt-quant/etl/task"
        headers = {"Content-Type": "application/json"}
        body = {
            "secu_codes": secu_codes,
            "start_date": start_date,
            "end_date": end_date,
            "category_code": category_code,
            "model_code": model_code,
            "is_concurrent_query": is_concurrent_query,
            "is_concurrent_save": is_concurrent_save,
            "is_init": is_init
        }
        # 请求参数去除为空的字段
        for strip in ["", None, []]:
            dict_trip(body, strip)
        resp = cgi_request_sync(
            cls.get_domain() + rest, method="POST", data=body, headers=headers,
            timeout=Api.REQUEST_TIMEOUT, decoder=Api.decoder_resp)
        return resp

    @classmethod
    def get_task(cls, task_id):
        """获取异步后台任务详情信息"""
        rest = "/qt-quant/etl/taskInfo"
        query = dict_trip({"task_id": task_id})
        resp = cgi_request_sync(
            cls.get_domain() + rest, method="GET",
            timeout=cls.REQUEST_TIMEOUT,
            decoder=cls.decoder_resp, **query)
        return resp

    @classmethod
    def get_etl_record(cls, category_code=None, model_code=None, secu_codes=None,
                       start_date=None, end_date=None):
        """获取etl模型结果记录列表"""
        rest = "/qt-quant/etl/modelRecord"
        headers = {"Content-Type": "application/json"}
        end_date = end_date or date.today().strftime("%Y-%m-%d")
        body = {
            "model_code": model_code,
            "category_code": category_code,
            "limit": 10,
            "page": 1,
            "desc": True,
            "secu_codes": secu_codes,
            "start_date": start_date,
            "end_date": end_date
        }
        for strip in ["", None, []]:
            dict_trip(body, strip)
        resp = cgi_request_sync(
            cls.get_domain() + rest, method="POST", data=body, headers=headers,
            timeout=cls.REQUEST_TIMEOUT, decoder=cls.decoder_resp)
        return resp


def parse_args():
    """命令行参数解析"""
    parse = argparse.ArgumentParser(description="DS调度运行脚本")
    parse.add_argument("--request_url", type=str,
                       help="外部环境使用 --request_url https://xxxx.com")
    parse.add_argument("--env", type=str, default="local",
                       help="公司内部环境使用 --env local|sit|dev|demo|prd，会自动映射请求url。例：--env dev, 实际访问服务url为：https://dev.plato,iquantex.com")
    parse.add_argument('--retry_time', type=int, default=1,
                       help="轮询时间：单位（s）, 异步后台任务接口会以多长时间轮询触发一次请求，适用于耗时较长的ETL模型，不希望太频繁的请求API的模型可根据实际需要配置此参数")
    parse.add_argument('-t', '--timeout', type=int, default=_DEFAULT_REQUEST_TIMEOUT, help="接口超时时间")
    parse.add_argument("-c", "--category", type=str, default=None, help="模型分类代码，例：Portfolio")
    parse.add_argument("-m", "--model", type=str, default=None, help="模型代码, 例：CombPosition")
    parse.add_argument("--secu_code", type=str, default=None, help="证券代码, 例：SECXXXX")
    parse.add_argument("-s", "--start", type=str, help="开始时间，默认是：2020-01-01", default="2020-01-01")
    parse.add_argument("-e", "--end", type=str, help="结束时间，默认是：当天的实时日期",
                       default=date.today().strftime("%Y-%m-%d"))
    parse.add_argument('--concurrent_query', action='store_true', help='是否通过执行数据查询')
    parse.add_argument('--concurrent_save', action='store_true', help='是否通过并发进行数据存储')
    parse.add_argument('--init', action='store_true', help='是否初始化操作，会覆盖更新etl')
    return parse.parse_args()


def run():
    """脚本启动入口"""

    # 1. 命令行参数解析
    args = parse_args()
    Api.RUN_ENV = args.env
    Api.REQUEST_URL = args.request_url
    Api.REQUEST_TIMEOUT = args.timeout
    frame_log.warning("running env: %s" % Api.RUN_ENV)
    retry_time = args.retry_time
    category_code = args.category
    model_code = args.model
    secu_codes = args.secu_code
    start_date = args.start
    end_date = args.end

    # 2. 调用异步后台任务
    result = Api.run_task(
        category_code=category_code, model_code=model_code,
        secu_codes=secu_codes, start_date=start_date,
        end_date=end_date, is_concurrent_query=args.concurrent_query,
        is_concurrent_save=args.concurrent_save,
        is_init=args.init)
    task_id = result.get("task_id")
    status = result.get("status")
    message = result.get("message")
    if not task_id or not status:
        raise QtException(msg=f"run etl:{model_code} task failed")

    # 3. 循环查询后台任务运行状态
    while status == "Running":
        frame_log.warning(f"轮询时间：等待{retry_time}s")
        time.sleep(retry_time)  # 轮询时间等待
        result = Api.get_task(task_id)
        status = result.get("status", "")
        message = result.get("message", "")
    if status == "Failed":
        frame_log.error("task:%s is failed, errmsg:%s", task_id, message)
        raise QtException(msg=f"run etl:{model_code} task failed")

    # 4. 验证ETL数据是否存在
    result = Api.get_etl_record(
        category_code=category_code, model_code=model_code,
        secu_codes=secu_codes, start_date=start_date,
        end_date=end_date)
    if len(result) > 0:
        for item in result:
            # records = item.get("records", None)
            count = item.get("count", None)
            model_code = item.get("model_code", None)
            frame_log.info("ETL:%s run successful, records count:%s", model_code, count)
    else:
        frame_log.info("ETL:%s run failed", model_code)
    return


if __name__ == '__main__':
    run()
