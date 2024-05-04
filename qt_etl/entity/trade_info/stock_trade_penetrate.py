# vim set fileencoding=utf-8

from datetime import date, datetime
from typing import Optional, Union

from qt_etl.entity.fields import InstrumentType, Measure
from qt_etl.entity.portfolio import CombPositionPenetrate
from qt_etl.entity.trade_info.trade_info import TradeInfo
from qt_common.qt_logging import frame_log
__all__ = ["StockTradePenetrateInfo"]


class StockTradePenetrateInfo(TradeInfo):
    """股票交易穿透后数据"""

    @classmethod
    def fetch_data(cls, secu_code: str = None,
                   start_date: Optional[Union[datetime, date]] = None,
                   end_date: Optional[Union[datetime, date]] = None):
        df = CombPositionPenetrate.get_penetrate_trade_data(secu_code=secu_code, start_date=start_date,
                                                            end_date=end_date,
                                                            instrument_type=InstrumentType.STOCK.name)
        if not df.empty:
            df = df.apply(cls.cost_apply, axis=1)
            calc_columns = [Measure.QUANTITY, Measure.PREV_QUANTITY, Measure.COST, Measure.PREV_COST]
            df[calc_columns] = df[calc_columns].fillna(0)
            df = df.drop(columns=calc_columns)
            df[Measure.TRADING_FEE] = 0
        else:
            frame_log.warning('CombPositionPenetrate.get_penetrate_trade_data获取数据为空')


        return df

    @classmethod
    def cost_apply(cls, row):
        cost = row[Measure.COST] - row[Measure.PREV_COST]
        if cost < 0:
            row[Measure.SELL_AMOUNT] = cost
            row[Measure.BUY_AMOUNT] = 0
        else:
            row[Measure.SELL_AMOUNT] = 0
            row[Measure.BUY_AMOUNT] = cost
        return row


if __name__ == '__main__':
    StockTradePenetrateInfo.run_etl(is_init=True)
