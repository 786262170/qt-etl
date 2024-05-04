# vim set fileencoding=utf-8

from datetime import date, datetime
from typing import Optional, Union

from qt_etl.entity.fields import Dimension, InstrumentType, Measure
from qt_etl.entity.portfolio import CombPositionPenetrate
from qt_etl.entity.trade_info.trade_info import TradeInfo
from qt_common.qt_logging import frame_log

__all__ = ["BondTradePenetrateInfo"]


class BondTradePenetrateInfo(TradeInfo):
    """债券交易穿透后数据"""

    @classmethod
    def fetch_data(cls, secu_code: str = None,
                   start_date: Optional[Union[datetime, date]] = None,
                   end_date: Optional[Union[datetime, date]] = None):
        df = CombPositionPenetrate.get_penetrate_trade_data(secu_code=secu_code, start_date=start_date,
                                                            end_date=end_date,
                                                            instrument_type=InstrumentType.BOND.name)
        """
        债券
        交易数量=当天持仓数量-前一天持仓数量，交易数量>0即为买入，交易数量小于0即为卖出；
        交易金额settlement_amount=trade_amount=当天成本cost-前一天成本prev_cost,交易金额>0即为买入，交易数量小于0即为卖出；
        应计利息arc_int=0
        """
        if not df.empty:
            df[Measure.SETTLE_AMOUNT] = df[Measure.TRADE_AMOUNT] = df[Measure.COST] - df[Measure.PREV_COST]
            drop_columns = [Measure.QUANTITY, Measure.PREV_QUANTITY, Measure.COST, Measure.PREV_COST]
            df = df.drop(columns=drop_columns)
            df[Measure.TRADING_FEE] = 0
            # fillna
            fill_nan_columns = [Measure.SETTLE_AMOUNT, Measure.TRADE_AMOUNT, Measure.TRADE_NOMINAL]
            df[fill_nan_columns] = df[fill_nan_columns].fillna(0)

        else:
            frame_log.warning('CombPositionPenetrate.get_penetrate_trade_data获取数据为空')

        return df


if __name__ == '__main__':
    BondTradePenetrateInfo.run_etl(is_init=True)

    df = BondTradePenetrateInfo.get_data(cond={Dimension.BOOK_ID:'demo4'})
    print(df)
