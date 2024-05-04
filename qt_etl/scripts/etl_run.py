# vim set fileencoding=utf-8
"""运行模型etl-同步"""
import time
from datetime import date

from qt_common.utils import timing
from qt_etl.entity.factor import *
from qt_etl.entity.instruments import *
from qt_etl.entity.market_data import *
from qt_etl.entity.portfolio import *
from qt_etl.entity.asset import *
from qt_etl.entity.trade_info import *
from qt_etl.entity.scenario import *

year_2020 = date(2020, 1, 1)
year_2015 = date(2015, 1, 1)


@timing
def etl_finacial():
    FinancialIndicator.run_etl(is_init=True)
    BarraLevel1.run_etl(is_init=True)
    BarraFactorReturnCne6.run_etl(is_init=True)
    BarraFactorReturnCne5.run_etl(is_init=True)
    BarraCne5Level1.run_etl(is_init=True)


@timing
def etl_trade():
    # trade_info
    BondTradeInfo.run_etl(is_init=True, start_date=year_2020)
    StockTradeInfo.run_etl(is_init=True, start_date=year_2020)
    StockTradePenetrateInfo.run_etl(is_init=True, )
    BondTradePenetrateInfo.run_etl(is_init=True)


@timing
def etl_protfolio():
    # protfolio
    DownRelationPortfolio.run_etl(is_init=True)
    CombPosition.run_etl(is_init=True, start_date=year_2020)
    CombPositionPenetrate.run_etl(is_init=True, start_date=year_2020)
    StockIndexPortfolio.run_etl(is_init=True, start_date=year_2020, is_concurrent_query=True, is_concurrent_save=True)
    CombAsset.run_etl(is_init=True)
    BondPosition.run_etl(is_init=True)


@timing
def etl_market_data():
    # market_data
    BenchmarkDailyQuote.run_etl(is_init=True, start_date=year_2015, is_concurrent_query=True, is_concurrent_save=True)
    BondValCSI.run_etl(is_init=True, start_date=year_2020)
    BondValCNBD.run_etl(is_init=True, start_date=year_2020)
    IndustryClassificationMktData.run_etl(is_init=True, start_date=year_2020)
    StockDailyQuote.run_etl(is_init=True, start_date=year_2015, is_concurrent_query=True)
    YieldCurveCNBDSample.run_etl(is_init=True, start_date=year_2020)
    IndexRate.run_etl(is_init=True, start_date=year_2020)
    TimeSeries.run_etl(is_init=True, start_date=year_2020)
    QtCalendar.run_etl(is_init=True)
    BondDailyQuote.run_etl(is_init=True, start_date=year_2020, is_concurrent_query=True)
    FundDailyQuote.run_etl(is_init=True)
    IssuerRating.run_etl(is_init=True)
    InstrumentRating.run_etl(is_init=True)
    BenchmarkDailyIndustryReturn.run_etl(is_init=True, start_date=year_2020)
    FxExchRate.run_etl(is_init=True)
    IssuerRating.run_etl(is_init=True, start_date=year_2020)
    TimeSeries.run_etl(is_init=True, start_date=year_2020)




@timing
def etl_instruments():
    BondInfo.run_etl(is_init=True, start_date=year_2020)
    FundInfo.run_etl(is_init=True, start_date=year_2020)
    BenchMarkInfo.run_etl(is_init=True, start_date=year_2020)
    StockIndexInfo.run_etl(is_init=True, start_date=year_2020)
    StockInfo.run_etl(is_init=True, start_date=year_2020)

@timing
def etl_asset():
    AssetClassification.run_etl(is_init=True)
    AssetClassificationRelation.run_etl(is_init=True)

@timing
def run_etl():
    # 需要先跑etl_protfolio
    etl_protfolio()
    etl_instruments()
    etl_trade()
    etl_market_data()
    etl_finacial()
    etl_asset()
    ScenarioDef.run_etl(is_init=True, start_date=year_2020)


if __name__ == '__main__':
    start_time = time.time()
    run_etl()
    print(f'total_time:{time.time() - start_time}')
