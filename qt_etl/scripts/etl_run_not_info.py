import time
from datetime import date

from qt_common.utils import timing
from qt_etl.entity.instruments import *
from qt_etl.entity.market_data import *
from qt_etl.entity.portfolio import *
from qt_etl.entity.trade_info import *
from qt_etl.entity.scenario import ScenarioDef

not_info_trade = [BondTradeInfo, BondTradePenetrateInfo, StockTradeInfo, StockTradePenetrateInfo]
not_info_portfolio = [CombPosition]
not_info_market = [IndexRate, QtCalendar]
not_info_instrument = [FundInfo]
not_info_tables = not_info_trade + not_info_portfolio + not_info_market + not_info_instrument


year_2020 = date(2020, 1, 1)
year_2015 = date(2015, 1, 1)


@timing
def etl_trade():
    # trade_info
    BondTradeInfo.run_etl(is_init=True, start_date=year_2020)
    StockTradeInfo.run_etl(is_init=True, )
    StockTradePenetrateInfo.run_etl(is_init=True, )
    BondTradePenetrateInfo.run_etl(is_init=True, )


@timing
def etl_protfolio():
    # protfolio
    CombPosition.run_etl(is_init=True, start_date=year_2020)
    CombPositionPenetrate.run_etl(is_init=True)
    DownRelationPortfolio.run_etl(is_init=True)
    CombAsset.run_etl(is_init=True)
    BondPosition.run_etl(is_init=True)

@timing
def etl_market_data():
    # market_data
    StockDailyQuote.run_etl(is_init=True, start_date=year_2015, is_concurrent_query=True)
    BenchmarkDailyIndustryReturn.run_etl(is_init=True, start_date=year_2020)
    FxExchRate.run_etl(is_init=True)

@timing
def etl_instruments():
    FundInfo.run_etl(is_init=True)
    BenchMarkInfo.run_etl(is_init=True)


@timing
def script_run_etl():
    etl_protfolio()
    etl_instruments()
    etl_trade()
    etl_market_data()
    ScenarioDef.run_etl(is_init=True, start_date=year_2020)


if __name__ == '__main__':
    start_time = time.time()
    script_run_etl()
    print(f'total_time:{time.time() - start_time}')
