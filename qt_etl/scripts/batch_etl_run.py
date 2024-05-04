# vim set fileencoding=utf-8
"""运行模型etl-并发"""

import concurrent.futures
import functools
import time
from datetime import date

from qt_common.qt_logging import frame_log
from qt_etl.entity.factor import *
from qt_etl.entity.fields import Dimension, InstrumentType
from qt_etl.entity.instruments import *
from qt_etl.entity.market_data import *
from qt_etl.entity.portfolio import *
from qt_etl.entity.trade_info import *

year_2020 = date(2020, 1, 1)
year_2015 = date(2015, 1, 1)

# 需要定义每个模型的运行参数
model_params_data = {
    "StockTradePenetrate": {},
    "BondTrade": {"start_date": year_2020, },
    "StockTrade": {"start_date": year_2020, },
    "StockIndexPortfolio": {"start_date": year_2020, "is_concurrent_query": True,
                            'is_concurrent_save': True},
    "CombPosition": {"start_date": year_2020, },
    "BondPosition": {"start_date": year_2020, },
    "IndustryClassificationMktData": {"start_date": year_2020},
    "StockDailyQuote": {"start_date": year_2015, "is_concurrent_query": True},
    "IndexRate": {"start_date": year_2020},
    "QtCalendar": {},
    "BondDailyQuote": {"start_date": year_2020, "is_concurrent_query": True},
    "FundDailyQuote": {},
    "BondInfo": {},
    "BenchMarkInfo": {},
    "FundInfo": {"start_date": year_2020},
    "StockIndexInfo": {"start_date": year_2020},
    "StockInfo": {"start_date": year_2020},
    "PartyRatingInfo": {"start_date": year_2020},
    "BarraLevel1": {},
    "FinancialIndicator": {},
    "BarraFactorReturnCne6": {},
    "BenchmarkDailyQuote": {"start_date": year_2015, "is_concurrent_query": True, 'is_concurrent_save': True},

}

finacial_models = [FinancialIndicator, BarraLevel1, BarraFactorReturnCne6]
trade_models = [BondTradeInfo, StockTradeInfo, StockTradePenetrateInfo]
protfolio_models = [StockIndexPortfolio, CombPosition,
                    DownRelationPortfolio]
mkt_models = [BenchmarkDailyQuote, IndustryClassificationMktData, StockDailyQuote, IndexRate,
              QtCalendar,
              BondDailyQuote, FundDailyQuote]
instruments_models = [BondInfo, FundInfo, StockIndexInfo, IssuerRating, BenchMarkInfo]

total_models = finacial_models + trade_models + protfolio_models + mkt_models + instruments_models


def bond_mkt_etl(is_init):
    """
    bond_mkt elt(BondValCSI依赖 CombPosition etl, 单独定义函数)
    :param is_init: 是否初始化（true：删除原来的文件）
    :return:
    """
    CombPosition.run_etl(start_date=year_2020)
    df_position = CombPosition.get_data(start_date=year_2020)
    bond_code_list = df_position[df_position[Dimension.INSTRUMENT_TYPE]==InstrumentType.BOND.name][Dimension.INSTRUMENT_CODE].unique().tolist()
    BondValCSI.run_etl(secu_codes=bond_code_list, start_date=year_2020, is_init=is_init)
    BondValCNBD.run_etl(secu_codes=bond_code_list, start_date=year_2020, is_init=is_init)


def run_model_etl(model, is_init):
    """
    运行单个模型etl
    :param model: 要运行etl的模型
    :param is_init: 是否初始化（true：删除原来的文件）
    :return:
    """
    model_name = model.__name__
    if model_name == CombPosition.__name__:
        return bond_mkt_etl(is_init=is_init)
    else:
        run_params = model_params_data.get(model_name, None)
        if run_params is None:
            raise Exception(f'{model} model未定义 etl param')
        else:
            run_params['is_init'] = is_init
        return functools.partial(getattr(model, 'run_etl'), **run_params)()


def run_etl(is_init=False):
    """
    运行etl
    :param is_init: bool 是否初始化（true：删除原来的文件）
    :return:
    """
    error_models = []
    success_models = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_tasks = {executor.submit(run_model_etl, _model, is_init): _model.__name__ for _model in
                        total_models}
        for futrue in concurrent.futures.as_completed(future_tasks):
            model_name = future_tasks[futrue]
            try:
                data = futrue.result()
                print(f'model res:{data}')
            except Exception as exc:
                frame_log.error('%r generated an exception: %s' % (model_name, exc))
                error_models.append(model_name)
            else:
                success_models.append(model_name)
    return {
        "success_models": success_models,
        "error_models": error_models
    }


if __name__ == '__main__':
    start_time = time.time()
    res = run_etl(is_init=True)
    print(res)
    print(f'total_time:{time.time() - start_time}')
