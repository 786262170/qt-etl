# vim set fileencoding=utf-8

import qtlib

from qt_common.error import QtException, QtError
from qt_etl.entity.entity_base import EntityBase
from qt_etl.entity.fields import InstrumentType, Dimension


class Instrument(EntityBase):
    """基本信息"""
    partitioned_by_date = False

    @classmethod
    def to_cpp_objs(cls, df):
        return df.apply(cls.to_cpp_obj, axis=1).to_numpy().tolist()

    @classmethod
    def to_cpp_obj(cls, row):
        if row[Dimension.INSTRUMENT_TYPE] == InstrumentType.FUND.name:
            try:
                return qtlib.Fund(row[Dimension.INSTRUMENT_CODE], row[Dimension.INSTRUMENT_NAME])
            except Exception as e:
                raise QtException(QtError.E_OTHER_BASE, message=e)
        elif row[Dimension.INSTRUMENT_TYPE] == InstrumentType.BENCH_MARK.name:
            try:
                return qtlib.Fund(row[Dimension.INSTRUMENT_CODE], row[Dimension.INSTRUMENT_NAME])
            except Exception as e:
                raise QtException(QtError.E_OTHER_BASE, message=e)

        elif row[Dimension.INSTRUMENT_TYPE] == InstrumentType.BOND.name:
            try:
                # TODO: interestOnFeb29
                # firstInterestDate 首次付息日需要查现金流获取，但是债券计算是不需要的，所以这个字段可以不传
                # 来自JAVA: 如果是零息债并且发行价格小于100，则转为贴现债进行计算
                bond_type = row[Dimension.BOND_TYPE]
                if row[Dimension.BOND_TYPE] == qtlib.BondType.ZeroCouponBond and row[Dimension.ISSUE_PRICE] < 100:
                    bond_type = qtlib.BondType.DiscountBond
                # 优先取预估到期日，如果预估到期日为空，则取实际到期日
                maturity_date = row[Dimension.ESTIMATED_MATURITY_DATE]
                if row[Dimension.ESTIMATED_MATURITY_DATE] is None:
                    maturity_date = row[Dimension.MATURITY_DATE]
                pay_frequency = row[Dimension.PAY_FREQUENCY]
                if row[Dimension.PAY_FREQUENCY] is None and (
                        bond_type == qtlib.BondType.DiscountBond or bond_type == qtlib.BondType.ZeroCouponBond):
                    pay_frequency = qtlib.FREQUENCY.Once.name
                if row[Dimension.INTEREST_START_DATE] is not None:
                    interest_start_date_list = sorted(list(row[Dimension.INTEREST_START_DATE]))

                    interest_start_date_filter = list(
                        filter(lambda x: x <= row[Dimension.SETTLE_DATE], interest_start_date_list))
                    interest_end_date_filter = list(row[Dimension.INTEREST_END_DATE])[:len(interest_start_date_filter)]
                    interest_period_interest_rate_filter = list(row[Dimension.INTEREST_PERIOD_INTEREST_RATE])[
                                                           :len(interest_start_date_filter)]
                    interest_period_interest_rate_filter = interest_period_interest_rate_filter
                else:
                    interest_start_date_filter = interest_end_date_filter = interest_period_interest_rate_filter = []

                return qtlib.init_bond(row[Dimension.CALC_WAY], row[Dimension.SETTLE_DATE], bond_type,
                                       row[Dimension.TRADING_MARKET], row[Dimension.START_DATE],
                                       maturity_date, row[Dimension.ISSUE_PRICE],
                                       [row[Dimension.PAR_VALUE]], [row[Dimension.PAR_RATE_ISSUE]],
                                       pay_frequency, row[Dimension.DAY_COUNT],
                                       row[Dimension.COMPOUNDING], "",
                                       row[Dimension.BENCHMARK_CODE], row[Dimension.SPREAD], 1,
                                       row[Dimension.BENCHMARK_DATES],
                                       row[Dimension.BENCHMARK_VALUES], row[Dimension.FLOAT_RATE_RECORDS],
                                       interest_start_date_filter, interest_end_date_filter,
                                       interest_period_interest_rate_filter,
                                       row[Dimension.NOTIONAL_REDUCE_MAP], row[Dimension.COUPON_CHANGE_MAP],
                                       row[Dimension.CALL_OPTION_MAP], row[Dimension.PUT_OPTION_MAP],
                                       row['forwardMaturityCurve'], row['forwardSpotCurve'],
                                       row[Dimension.HIGH_INTEREST_RATE_ADJ], row[Dimension.LOW_INTEREST_RATE_ADJ])
            except Exception as e:
                raise QtException(QtError.E_OTHER_BASE, message=e)

        else:
            raise NotImplemented(f'{cls.__name__}')

    @classmethod
    def get_data(cls, **kwargs):
        """Instrument info not support cond :{"start_date": "xxx", "end_date": "xxx"}"""
        if "start_date" in kwargs:
            kwargs.pop("start_date", None)
        if "end_date" in kwargs:
            kwargs.pop("end_date", None)
        return super(Instrument, cls).get_data(**kwargs)
