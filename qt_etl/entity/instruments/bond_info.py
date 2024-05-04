# vim set fileencoding=utf-8
import json
from datetime import date, datetime
from typing import Optional, Union

import numpy as np
import pyarrow as pa
import qtlib

from qt_common.utils import str_to_date, date_to_str
from qt_etl.entity.fields import InstrumentType, Dimension
from qt_etl.entity.instruments.instrument import Instrument

__all__ = ['BondInfo']


class BondInfo(Instrument):
    """债券基本信息"""
    main_table = 'INFO_FI_BASICINFO'
    partition_max_rows_per_file = 100000
    code_source_table = {
        "INFO_FI_BASICINFO": "BOND_CODE",
        "INFO_FI_ADVANCE_REPAY": "BOND_CODE",
        "INFO_FI_RIT_EMB_XCS": "BOND_CODE",
        "INFO_FI_IR_DETAILED": "BOND_CODE",
    }

    schema = pa.schema([
        pa.field(Dimension.INSTRUMENT_CODE, pa.string(),
                 metadata={b'source_table': json.dumps(code_source_table).encode()}),
        pa.field(Dimension.INSTRUMENT_NAME, pa.string(),
                 metadata={b'table_field': b'CSNAME', b'table_name': b'INFO_FI_BASICINFO'}),
        pa.field(Dimension.SYMBOL, pa.string(),
                 metadata={b'table_field': b'BOND_SYMBOL', b'table_name': b'INFO_FI_BASICINFO'}),
        pa.field(Dimension.BOND_TERM_DAY, pa.float64(),
                 metadata={b'table_field': b'BOND_TERM_DAY', b'table_name': b'INFO_FI_BASICINFO'}),
        pa.field(Dimension.BOND_TERM_YEAR, pa.float64(),
                 metadata={b'table_field': b'BOND_TERM_YEAR', b'table_name': b'INFO_FI_BASICINFO'}),
        pa.field(Dimension.PAR_VALUE_ISS, pa.float64(),
                 metadata={b'table_field': b'PAR_VALUE_ISS', b'table_name': b'INFO_FI_BASICINFO'}),
        pa.field(Dimension.TRADING_MARKET, pa.string(),
                 metadata={b'table_field': b'EXCH_CODE', b'table_name': b'INFO_FI_BASICINFO'}),
        pa.field(Dimension.ESTIMATED_MATURITY_DATE, pa.string(),
                 metadata={b'table_field': b'XPC_DUE_DATE', b'table_name': b'INFO_FI_BASICINFO'}),
        pa.field(Dimension.MATURITY_DATE, pa.string(),
                 metadata={b'table_field': b'ACT_DUE_DATE', b'table_name': b'INFO_FI_BASICINFO'}),
        pa.field(Dimension.ISS_START_DATE, pa.string(),
                 metadata={b'table_field': b'ISS_START_DATE', b'table_name': b'INFO_FI_BASICINFO'}),
        pa.field(Dimension.DAY_COUNT, pa.string(),
                 metadata={b'table_field': b'INT_RULE', b'table_name': b'INFO_FI_BASICINFO'}),
        pa.field(Dimension.START_DATE, pa.string(),
                 metadata={b'table_field': b'VALUE_DATE', b'table_name': b'INFO_FI_BASICINFO'}),
        pa.field(Dimension.ISSUE_PRICE, pa.float64(),
                 metadata={b'table_field': b'ISS_PRICE', b'table_name': b'INFO_FI_BASICINFO'}),
        pa.field(Dimension.ISSUER_CODE, pa.string(),
                 metadata={b'table_field': b'PARTY_CODE', b'table_name': b'INFO_FI_BASICINFO'}),
        pa.field(Dimension.PAR_RATE_ISSUE, pa.float64(),
                 metadata={b'table_field': b'PAR_ANN_IR', b'table_name': b'INFO_FI_BASICINFO'}),
        pa.field(Dimension.COUPON_TYPE, pa.string(),
                 metadata={b'table_field': b'CPN_TYPE', b'table_name': b'INFO_FI_BASICINFO'}),
        pa.field(Dimension.INTEREST_RATE_TYPE, pa.string(),
                 metadata={b'table_field': b'IR_TYPE', b'table_name': b'INFO_FI_BASICINFO'}),
        pa.field(Dimension.BENCHMARK_CODE, pa.string(),
                 metadata={b'table_field': b'FLT_BM_CODE', b'table_name': b'INFO_FI_BASICINFO'}),
        pa.field(Dimension.SPREAD, pa.float64(),
                 metadata={b'table_field': b'FLT_IR_SPRD', b'table_name': b'INFO_FI_BASICINFO'}),
        pa.field(Dimension.PAY_FREQUENCY, pa.string(),
                 metadata={b'table_field': b'PAY_FREQ', b'table_name': b'INFO_FI_BASICINFO'}),
        pa.field(Dimension.ANN_IR_FLOOR, pa.float64(),
                 metadata={b'table_field': b'ANN_IR_FLOOR', b'table_name': b'INFO_FI_BASICINFO'}),
        pa.field(Dimension.CALC_WAY, pa.int64(),
                 metadata={b'table_field': b'CALC_WAY', b'table_name': b'INFO_FI_ADVANCE_REPAY'}),
        pa.field(Dimension.FRN_ADJUST_RT, pa.int64(),
                 metadata={b'table_field': b'INT_ADJ_MODE', b'table_name': b'INFO_FI_BASICINFO'}),
        pa.field(Dimension.PAR_VALUE, pa.float64(),
                 metadata={b'table_field': b'PAR_VALUE', b'table_name': b'INFO_FI_BASICINFO'}),
        pa.field(Dimension.REFERENCE_RATE_ISSUE, pa.float64(), metadata={b'table_field': b'REF_FLD'}),
        pa.field(Dimension.INSTRUMENT_TYPE, pa.string(), metadata={b'table_field': b''}),
        pa.field(Dimension.COUPON_ACCURACY, pa.int64(), metadata={b'table_field': b''}),
        pa.field(Dimension.IS_CREATE_NEW, pa.int64(), metadata={b'table_field': b''}),
        pa.field(Dimension.NOTIONAL_REDUCE_MAP, pa.string(), metadata={b'table_field': b''}),
        pa.field(Dimension.CALL_OPTION_MAP, pa.string(), metadata={b'table_field': b''}),
        pa.field(Dimension.PUT_OPTION_MAP, pa.string(), metadata={b'table_field': b''}),
        pa.field(Dimension.HIGH_INTEREST_RATE_ADJ, pa.string(), metadata={b'table_field': b''}),
        pa.field(Dimension.LOW_INTEREST_RATE_ADJ, pa.string(), metadata={b'table_field': b''}),
        pa.field(Dimension.ADJUST_NUM_FOR_LASTEST, pa.float64(), metadata={b'table_field': b''}),
        pa.field(Dimension.INTEREST_START_DATE, pa.list_(pa.string()), metadata={b'table_field': b''}),
        pa.field(Dimension.INTEREST_END_DATE, pa.list_(pa.string()), metadata={b'table_field': b''}),
        pa.field(Dimension.INTEREST_PERIOD_INTEREST_RATE, pa.list_(pa.float64()),
                 metadata={b'table_field': b''}),
        pa.field(Dimension.COUPON_CHANGE_MAP, pa.string(), metadata={b'table_field': b''}),
        pa.field(Dimension.DELIST_DATE, pa.string(), metadata={b'table_field': b'DELIST_DATE'}),
        pa.field(Dimension.FLOAT_RATE_RECORDS, pa.list_(pa.float64()), metadata={b'table_field': b''}),
        pa.field(Dimension.CLS_CODE_1ST, pa.string(), metadata={b'table_field': b'CLS_CODE_1ST', b'table_name': b'INFO_ZG_SEC_CLASSIFICATION'}),
        pa.field(Dimension.CLS_CODE_2ND, pa.string(), metadata={b'table_field': b'CLS_CODE_2ND', b'table_name': b'INFO_ZG_SEC_CLASSIFICATION'}),
        pa.field(Dimension.CLS_CODE_3RD, pa.string(), metadata={b'table_field': b'CLS_CODE_3RD', b'table_name': b'INFO_ZG_SEC_CLASSIFICATION'}),
        pa.field(Dimension.CLS_CODE_4TH, pa.string(), metadata={b'table_field': b'CLS_CODE_4TH', b'table_name': b'INFO_ZG_SEC_CLASSIFICATION'}),
    ],
        metadata={
            Dimension.INSTRUMENT_CODE: '债券内部编码',
            Dimension.INSTRUMENT_NAME: '债券简称',
            Dimension.SYMBOL: '债券代码',
            Dimension.BOND_TERM_DAY: '债券期限(天)',
            Dimension.BOND_TERM_YEAR: '债券期限(年)',
            Dimension.PAR_VALUE_ISS: '',
            Dimension.TRADING_MARKET: '交易所编码',
            Dimension.ESTIMATED_MATURITY_DATE: '预估到期日期',
            Dimension.MATURITY_DATE: '实际到期日期',
            Dimension.ISS_START_DATE: '发行起始日',
            Dimension.DAY_COUNT: '计息规则',
            Dimension.START_DATE: '计息起始日',
            Dimension.ISSUE_PRICE: '发行价格',
            Dimension.ISSUER_CODE: '发行人代码',
            Dimension.PAR_RATE_ISSUE: '发行时票面利率',
            Dimension.COUPON_TYPE: '付息方式',
            Dimension.INTEREST_RATE_TYPE: '利率类型',
            Dimension.BENCHMARK_CODE: '浮息债基准利率',
            Dimension.SPREAD: '浮息债利差',
            Dimension.PAY_FREQUENCY: '付息周期',
            Dimension.ANN_IR_FLOOR: '债券保底利率',
            Dimension.CALC_WAY: '付息周期小于一年计息方法',
            Dimension.FRN_ADJUST_RT: '浮息债调息时间类型',
            Dimension.PAR_VALUE: '',
            Dimension.REFERENCE_RATE_ISSUE: '',
            Dimension.INSTRUMENT_TYPE: '',
            Dimension.COUPON_ACCURACY: '',
            Dimension.IS_CREATE_NEW: '',
            Dimension.NOTIONAL_REDUCE_MAP: '',
            Dimension.CALL_OPTION_MAP: '',
            Dimension.PUT_OPTION_MAP: '',
            Dimension.HIGH_INTEREST_RATE_ADJ: '',
            Dimension.LOW_INTEREST_RATE_ADJ: '',
            Dimension.ADJUST_NUM_FOR_LASTEST: '',
            Dimension.INTEREST_START_DATE: '',
            Dimension.INTEREST_END_DATE: '',
            Dimension.INTEREST_PERIOD_INTEREST_RATE: '',
            Dimension.COUPON_CHANGE_MAP: '',
            Dimension.FLOAT_RATE_RECORDS: '',
            Dimension.SECU_TYPE_CODE: '',
            Dimension.CLS_CODE_1ST: '债券信用类别1级',
            Dimension.CLS_CODE_2ND: '债券信用类别2级',
            Dimension.CLS_CODE_3RD: '债券信用类别3级',
            Dimension.CLS_CODE_4TH: '债券信用类别4级',
            Dimension.DELIST_DATE: '退市日期'
        }
    )

    @classmethod
    def fetch_data(cls,
                   secu_code: str = None,
                   start_date: Optional[Union[datetime, date]] = None,
                   end_date: Optional[Union[datetime, date]] = None):
        df_bond_basic_data = cls.fetch_bond_basic_data(secu_code, start_date, end_date)
        df_bond_basic_data.set_index(Dimension.INSTRUMENT_CODE, inplace=True)
        df_bond_basic_data[Dimension.COUPON_ACCURACY] = 4  # 直接赋值
        # TODO:
        df_bond_basic_data[Dimension.IS_CREATE_NEW] = 1  # 依赖于浮息债基准利率 FLT_BM

        df_bond_advance_repay_data = cls.fetch_bond_advance_repay_data(secu_code, start_date, end_date)
        df_bond_basic_data[Dimension.NOTIONAL_REDUCE_MAP] = df_bond_advance_repay_data.groupby(
            [Dimension.INSTRUMENT_CODE]).apply(
            lambda x: json.dumps(dict(zip(x[Dimension.TRADE_DATE], x[Dimension.ADVANCE_REPAY_RATIO]))))
        df_bond_right_data = cls.fetch_bond_right_data(secu_code, start_date, end_date)
        df_bond_basic_data[Dimension.CALL_OPTION_MAP] = df_bond_right_data.loc[
            df_bond_right_data[Dimension.RIT_TYPE] == '002'].groupby([Dimension.INSTRUMENT_CODE]).apply(
            lambda x: json.dumps(dict(zip(x[Dimension.TRADE_DATE], x[Dimension.STRIKE_PRICE]))))
        df_bond_basic_data[Dimension.PUT_OPTION_MAP] = df_bond_right_data.loc[
            df_bond_right_data[Dimension.RIT_TYPE] == '001'].groupby([Dimension.INSTRUMENT_CODE]).apply(
            lambda x: json.dumps(dict(zip(x[Dimension.TRADE_DATE], x[Dimension.STRIKE_PRICE]))))
        df_bond_right_data[Dimension.HIGH_INTEREST_RATE_ADJ].fillna(100000, inplace=True)
        df_bond_right_data[Dimension.LOW_INTEREST_RATE_ADJ].fillna(100000, inplace=True)
        df_bond_basic_data[Dimension.HIGH_INTEREST_RATE_ADJ] = df_bond_right_data.groupby(
            [Dimension.INSTRUMENT_CODE]).apply(
            lambda x: json.dumps(dict(zip(x[Dimension.TRADE_DATE], x[Dimension.HIGH_INTEREST_RATE_ADJ]))))
        df_bond_basic_data[Dimension.LOW_INTEREST_RATE_ADJ] = df_bond_right_data.groupby(
            [Dimension.INSTRUMENT_CODE]).apply(
            lambda x: json.dumps(dict(zip(x[Dimension.TRADE_DATE], x[Dimension.LOW_INTEREST_RATE_ADJ]))))
        df_bond_interest_rate_data = cls.fetch_bond_interest_rate(secu_code, start_date, end_date)
        df_bond_interest_rate_data[Dimension.BASE_INTEREST_RATE].fillna(0, inplace=True)
        df_bond_interest_rate_data[Dimension.INTEREST_PERIOD_INTEREST_RATE].fillna(0, inplace=True)
        df_bond_basic_data[Dimension.ADJUST_NUM_FOR_LASTEST] = (
                df_bond_basic_data[Dimension.START_DATE].apply(
                    str_to_date) - df_bond_interest_rate_data.sort_values(by=[Dimension.INSTRUMENT_CODE,
                                                                              Dimension.INTEREST_START_DATE]).drop_duplicates(
            subset=[Dimension.INSTRUMENT_CODE], keep='first').set_index(Dimension.INSTRUMENT_CODE)[
                    Dimension.INTEREST_START_DATE].apply(str_to_date)).apply(
            lambda x: float(x.days))  # TODO: workday

        df_bond_interest_rate_data_instrument = df_bond_interest_rate_data.groupby(
            [Dimension.INSTRUMENT_CODE])
        df_bond_basic_data[Dimension.ADJUST_NUM_FOR_LASTEST].fillna(0, inplace=True)
        df_bond_interest_rate_data.set_index(Dimension.INSTRUMENT_CODE, inplace=True)

        df_bond_basic_data[Dimension.INTEREST_START_DATE] = df_bond_interest_rate_data_instrument.apply(
            lambda x: x[Dimension.INTEREST_START_DATE].to_list())
        df_bond_basic_data[Dimension.INTEREST_END_DATE] = df_bond_interest_rate_data_instrument.apply(
            lambda x: x[Dimension.INTEREST_END_DATE].to_list())
        df_bond_basic_data[
            Dimension.INTEREST_PERIOD_INTEREST_RATE] = df_bond_interest_rate_data_instrument.apply(
            lambda x: x[Dimension.INTEREST_PERIOD_INTEREST_RATE].to_list())
        df_bond_basic_data[Dimension.COUPON_CHANGE_MAP] = df_bond_interest_rate_data_instrument.apply(
            lambda x: json.dumps(
                dict(zip(x[Dimension.INTEREST_START_DATE], x[Dimension.INTEREST_PERIOD_INTEREST_RATE]))))

        df_bond_basic_data[Dimension.FLOAT_RATE_RECORDS] = df_bond_basic_data.apply(
            lambda x: x[[Dimension.COUPON_ACCURACY, Dimension.IS_CREATE_NEW,
                         Dimension.ADJUST_NUM_FOR_LASTEST, Dimension.FRN_ADJUST_RT,
                         Dimension.CALC_WAY]].values.tolist(), axis=1)
        df_bond_basic_data = df_bond_basic_data.reset_index()

        df_bond_credit_sector = cls.fetch_bond_credit_sector()
        df_bond_credit_sector.rename(columns={
            'SEC_CODE': Dimension.INSTRUMENT_CODE
        }, inplace=True)
        df_bond_basic_data = df_bond_basic_data.merge(df_bond_credit_sector, how='left', on=Dimension.INSTRUMENT_CODE)
        df_bond_basic_data = df_bond_basic_data.reset_index(drop=True)

        return df_bond_basic_data

    @classmethod
    def fetch_bond_basic_data(cls,
                              secu_code: str = None,
                              start_date: Optional[Union[datetime, date]] = None,
                              end_date: Optional[Union[datetime, date]] = None):
        sql = f"""SELECT
                    INFO_FI_BASICINFO.BOND_CODE,
                    INFO_FI_BASICINFO.CSNAME,
                    INFO_FI_BASICINFO.BOND_SYMBOL,
                    INFO_FI_BASICINFO.BOND_TERM_DAY,
                    INFO_FI_BASICINFO.BOND_TERM_YEAR,
                    INFO_FI_BASICINFO.PAR_VALUE_ISS,
                    INFO_FI_BASICINFO.EXCH_CODE,
                    INFO_FI_BASICINFO.XPC_DUE_DATE,
                    INFO_FI_BASICINFO.ACT_DUE_DATE,
                    INFO_FI_BASICINFO.ISS_START_DATE,
                    INFO_FI_BASICINFO.INT_RULE,
                    INFO_FI_BASICINFO.VALUE_DATE,
                    INFO_FI_BASICINFO.ISS_PRICE,
                    INFO_FI_BASICINFO.PARTY_CODE,
                    INFO_FI_BASICINFO.PAR_ANN_IR,
                    INFO_FI_BASICINFO.CPN_TYPE,
                    INFO_FI_BASICINFO.IR_TYPE,
                    INFO_FI_BASICINFO.FLT_BM_CODE,
                    INFO_FI_BASICINFO.FLT_IR_SPRD,
                    INFO_FI_BASICINFO.PAY_FREQ,
                    INFO_FI_BASICINFO.ANN_IR_FLOOR,
                    INFO_FI_BASICINFO.INT_FML,
                    INFO_FI_BASICINFO.INT_ADJ_MODE,
                    INFO_FI_BASICINFO.DELIST_DATE,
                    INFO_FI_BONDISSUE.PAR_VALUE,
                    INFO_FI_BONDISSUE.REF_FLD
                FROM
                    INFO_FI_BASICINFO
                LEFT JOIN INFO_FI_BONDISSUE
                    ON INFO_FI_BASICINFO.BOND_CODE = INFO_FI_BONDISSUE.BOND_CODE
                    where ISS_MODE='001'
                                """
        df = cls.query(sql)
        df.rename(columns=cls.etl_rename_dict, inplace=True)
        df = df[~df[Dimension.MATURITY_DATE].isna()]
        df[Dimension.SYMBOL] = df[Dimension.SYMBOL] + '.' + df[Dimension.TRADING_MARKET]
        df[Dimension.BOND_TERM_DAY] = df[Dimension.BOND_TERM_DAY].astype(float)
        df[Dimension.BOND_TERM_YEAR] = df[Dimension.BOND_TERM_YEAR].astype(float)
        df[Dimension.DAY_COUNT] = df[Dimension.DAY_COUNT].map(cls.day_count_mapping)
        df[Dimension.ESTIMATED_MATURITY_DATE] = df[Dimension.ESTIMATED_MATURITY_DATE].apply(date_to_str)
        df[Dimension.MATURITY_DATE] = df[Dimension.MATURITY_DATE].apply(date_to_str)
        df[Dimension.START_DATE] = df[Dimension.START_DATE].apply(date_to_str)
        df[Dimension.ISS_START_DATE] = df[Dimension.ISS_START_DATE].apply(date_to_str)
        df[Dimension.DELIST_DATE] = df[Dimension.DELIST_DATE].apply(date_to_str)

        df[Dimension.PAR_VALUE_ISS] = df[Dimension.PAR_VALUE_ISS].astype(float)
        df[Dimension.PAR_VALUE] = df[Dimension.PAR_VALUE].astype(float)
        df[Dimension.REFERENCE_RATE_ISSUE] = df[Dimension.REFERENCE_RATE_ISSUE].astype(float) / 100
        df[Dimension.INSTRUMENT_TYPE] = InstrumentType.BOND.name
        df[Dimension.ISSUE_PRICE] = df[Dimension.ISSUE_PRICE].astype(float)
        df[Dimension.PAR_RATE_ISSUE] = df[Dimension.PAR_RATE_ISSUE].astype(float) / 100

        df[Dimension.SPREAD] = df[Dimension.SPREAD].astype(float) / 100
        df[Dimension.PAY_FREQUENCY] = df[Dimension.PAY_FREQUENCY].map(cls.pay_frequency_mapping)
        df[Dimension.COUPON_TYPE] = df[Dimension.COUPON_TYPE].map(cls.coupon_type_mapping)
        # 零息债 PAY_FREQUENCY=Once
        df.loc[df[
                   Dimension.COUPON_TYPE] == qtlib.BondType.ZeroCouponBond.name, Dimension.PAY_FREQUENCY] = qtlib.FREQUENCY.Once.name
        df[Dimension.INTEREST_RATE_TYPE] = df[Dimension.INTEREST_RATE_TYPE].map(cls.interest_rate_mapping)
        df[Dimension.ANN_IR_FLOOR] = df[Dimension.ANN_IR_FLOOR].astype(float)
        df[Dimension.FRN_ADJUST_RT] = df[Dimension.FRN_ADJUST_RT].map(cls.float_adj_mode_mapping)
        df[Dimension.FRN_ADJUST_RT].fillna(2, inplace=True)
        df[Dimension.FRN_ADJUST_RT] = df[Dimension.FRN_ADJUST_RT].astype(np.int64)
        df[Dimension.CALC_WAY] = df[Dimension.CALC_WAY].map(cls.calc_way_mapping)
        df[Dimension.CALC_WAY].fillna(9, inplace=True)
        df[Dimension.CALC_WAY] = df[Dimension.CALC_WAY].astype(np.int64)

        return df

    @classmethod
    def fetch_bond_advance_repay_data(cls,
                                      secu_code: str = None,
                                      start_date: Optional[Union[datetime, date]] = None,
                                      end_date: Optional[Union[datetime, date]] = None):
        sql = f"""SELECT
                            BOND_CODE,
                            ADV_PAY_MODE,
                            ADV_PAY_DATE,
                            ADV_PAY_RAT,
                            PAY_FIR_VALUE,
                            PAY_AFT_VALUE
                        FROM
                            INFO_FI_ADVANCE_REPAY
                        where ADV_PAY_MODE='001'
                        AND ADV_PAY_RAT is not null
                        AND ADV_PAY_DATE is not null
                        """
        df = cls.query(sql)
        if not df.empty:
            df = df.rename(columns=cls.etl_rename_dict)
            df[Dimension.TRADE_DATE] = df[Dimension.TRADE_DATE].apply(date_to_str)
            df[Dimension.ADVANCE_REPAY_RATIO] = df[Dimension.ADVANCE_REPAY_RATIO].astype(float) / 100
            df[Dimension.PAY_FIR_VALUE] = df[Dimension.PAY_FIR_VALUE].astype(float)
            df[Dimension.PAY_AFT_VALUE] = df[Dimension.PAY_AFT_VALUE].astype(float)
            df[Dimension.ADVANCE_REPAY_RATIO] = df[Dimension.ADVANCE_REPAY_RATIO].astype(float)
        return df

    @classmethod
    def fetch_bond_right_data(cls,
                              secu_code: str = None,
                              start_date: Optional[Union[datetime, date]] = None,
                              end_date: Optional[Union[datetime, date]] = None
                              ):
        """含权信息 call put"""
        sql = f"""SELECT
                            BOND_CODE,
                            RIT_TYPE,
                            XCS_THRT_DATE,
                            XCS_PRC,
                            HI_IR_ADJ,
                            LO_IR_ADJ
                        FROM
                            INFO_FI_RIT_EMB_XCS 
                        where DATA_TYPE='002' 
                        and XCS_THRT_DATE is not null
                        """
        df = cls.query(sql)
        if not df.empty:
            df = df.rename(columns=cls.etl_rename_dict)
            df[Dimension.TRADE_DATE] = df[Dimension.TRADE_DATE].apply(date_to_str)
            df[Dimension.STRIKE_PRICE] = df[Dimension.STRIKE_PRICE].apply(float)
            df[Dimension.HIGH_INTEREST_RATE_ADJ] = df[Dimension.HIGH_INTEREST_RATE_ADJ].astype(
                float) / 10000
            df[Dimension.LOW_INTEREST_RATE_ADJ] = df[Dimension.LOW_INTEREST_RATE_ADJ].astype(
                float) / 10000
        return df

    @classmethod
    def fetch_bond_interest_rate(cls,
                                 secu_code: str = None,
                                 start_date: Optional[Union[datetime, date]] = None,
                                 end_date: Optional[Union[datetime, date]] = None
                                 ):
        # 债券利率明细表 记录债券历次利率变动明细数据，包括固定利率、浮动利率、递进利率的每个计息周期利率。
        sql = f"""select
                        BOND_CODE,
                        IR_START_DATE,
                        IR_END_DATE,
                        BASE_IR,
                        IP_PAR_IR
                    FROM
                        INFO_FI_IR_DETAILED
                    """
        df = cls.query(sql)
        if not df.empty:
            df = df.rename(columns=cls.etl_rename_dict)
            df[Dimension.INTEREST_START_DATE] = df[Dimension.INTEREST_START_DATE].apply(date_to_str)
            df[Dimension.INTEREST_END_DATE] = df[Dimension.INTEREST_END_DATE].apply(date_to_str)
            df[Dimension.BASE_INTEREST_RATE] = df[Dimension.BASE_INTEREST_RATE].astype(float) / 100
            df[Dimension.INTEREST_PERIOD_INTEREST_RATE] = df[
                                                              Dimension.INTEREST_PERIOD_INTEREST_RATE].astype(
                float) / 100
        return df

    @classmethod
    def fetch_bond_credit_sector(cls,
                                 secu_code: str = None,
                                 start_date: Optional[Union[datetime, date]] = None,
                                 end_date: Optional[Union[datetime, date]] = None
                                 ):
        sql = f"""SELECT SEC_CODE,CLS_CODE_1ST,CLS_CODE_2ND,CLS_CODE_3RD,CLS_CODE_4TH FROM INFO_ZG_SEC_CLASSIFICATION"""
        df = cls.query(sql)
        df[Dimension.CLS_CODE_1ST] = df[Dimension.CLS_CODE_1ST].fillna('其他')
        df[Dimension.CLS_CODE_2ND] = df[Dimension.CLS_CODE_2ND].fillna('其他')
        df[Dimension.CLS_CODE_3RD] = df[Dimension.CLS_CODE_3RD].fillna('其他')
        df[Dimension.CLS_CODE_4TH] = df[Dimension.CLS_CODE_4TH].fillna('其他')

        return df

    etl_rename_dict = {
        'BOND_CODE': Dimension.INSTRUMENT_CODE,
        'CSNAME': Dimension.INSTRUMENT_NAME,
        'BOND_SYMBOL': Dimension.SYMBOL,
        'BOND_TERM_DAY': Dimension.BOND_TERM_DAY,
        'BOND_TERM_YEAR': Dimension.BOND_TERM_YEAR,
        'PAR_VALUE_ISS': Dimension.PAR_VALUE_ISS,
        'PAR_VALUE': Dimension.PAR_VALUE,
        'REF_FLD': Dimension.REFERENCE_RATE_ISSUE,
        'EXCH_CODE': Dimension.TRADING_MARKET,
        'XPC_DUE_DATE': Dimension.ESTIMATED_MATURITY_DATE,
        'ACT_DUE_DATE': Dimension.MATURITY_DATE,
        'ISS_START_DATE': Dimension.ISS_START_DATE,
        'INT_RULE': Dimension.DAY_COUNT,
        "VALUE_DATE": Dimension.START_DATE,
        "ISS_PRICE": Dimension.ISSUE_PRICE,
        "PARTY_CODE": Dimension.ISSUER_CODE,
        "PAR_ANN_IR": Dimension.PAR_RATE_ISSUE,
        "CPN_TYPE": Dimension.COUPON_TYPE,
        "IR_TYPE": Dimension.INTEREST_RATE_TYPE,
        "FLT_BM_CODE": Dimension.BENCHMARK_CODE,
        # "FLT_BM": Dimension.BENCHMARK,
        "FLT_IR_SPRD": Dimension.SPREAD,
        "PAY_FREQ": Dimension.PAY_FREQUENCY,
        'ADV_PAY_DATE': Dimension.TRADE_DATE,
        'ADV_PAY_MODE': Dimension.ADVANCE_REPAY_MODE,
        'ADV_PAY_RAT': Dimension.ADVANCE_REPAY_RATIO,
        'PAY_AFT_VALUE': Dimension.PAY_AFT_VALUE,
        'PAY_FIR_VALUE': Dimension.PAY_FIR_VALUE,
        'RIT_TYPE': Dimension.RIT_TYPE,
        'XCS_THRT_DATE': Dimension.TRADE_DATE,
        'XCS_PRC': Dimension.STRIKE_PRICE,
        'HI_IR_ADJ': Dimension.HIGH_INTEREST_RATE_ADJ,
        'LO_IR_ADJ': Dimension.LOW_INTEREST_RATE_ADJ,
        # 'XCS_IR_ADJ': Dimension.ADJUST_ISSUE_RATE,
        'IR_START_DATE': Dimension.INTEREST_START_DATE,
        'IR_END_DATE': Dimension.INTEREST_END_DATE,
        'BASE_IR': Dimension.BASE_INTEREST_RATE,
        'IP_PAR_IR': Dimension.INTEREST_PERIOD_INTEREST_RATE,
        'ANN_IR_FLOOR': Dimension.ANN_IR_FLOOR,  # 债券保底利率
        'INT_FML': Dimension.CALC_WAY,
        'INT_ADJ_MODE': Dimension.FRN_ADJUST_RT,
        'DELIST_DATE': Dimension.DELIST_DATE

    }

    day_count_mapping = {
        "001": "A/365",
        "002": "Act/Act",
        "003": "A/360",
        "004": "30/360",
        "006": "A/366",
        "007": "A/365F",
        "008": "AVG/ACT",
    }

    coupon_type_mapping = {
        # 001-零息债券，002-贴现债券，003-附息债券，999-其他
        "001": qtlib.BondType.ZeroCouponBond.name,
        "002": qtlib.BondType.DiscountBond.name,
        "003": "CouponBond",
        "999": "ELSE",
    }

    interest_rate_mapping = {
        # 001-固定利率，002-浮动利率，003-递进利率，999-其他
        "001": qtlib.BondType.FixedRateBond.name,
        "002": qtlib.BondType.FloatRateBond.name,
        "003": qtlib.BondType.FixedRateBond.name,
        "999": "ELSE",
    }

    pay_frequency_mapping = {
        # 001-每年付息，002-半年付息，003-到期一次还本付息，004-按季付息，005-按月付息，
        # 006-周期性付息，007-贴现，009-9个月一次，010-15天一次，
        # 008-4个月一次，011-2个月一次，999-其他；
        # 数据库中债券付息频率仅有001-005和空
        "001": qtlib.FREQUENCY.Annual.name,
        "002": qtlib.FREQUENCY.Semiannual.name,
        "003": qtlib.FREQUENCY.Once.name,
        "004": qtlib.FREQUENCY.Quarterly.name,
        "005": qtlib.FREQUENCY.Monthly.name,
        "006": qtlib.FREQUENCY.OtherFrequency.name,
        "007": qtlib.FREQUENCY.OtherFrequency.name,
        "008": qtlib.FREQUENCY.EveryFourthMonth.name,
        "009": qtlib.FREQUENCY.OtherFrequency.name,
        "010": qtlib.FREQUENCY.OtherFrequency.name,
        "011": qtlib.FREQUENCY.Bimonthly.name,
        "999": qtlib.FREQUENCY.NoFrequency.name,
    }

    float_adj_mode_mapping = {
        # 浮息债调息时间类型映射
        # 001 - 调息日始使用新利息（分段计息），002 - 整个计息期均为新利息，003 - 下一付息期使用新利息，005 - 调息日对应日，999 - 其他；
        # cpp 保留整数。1表示自调息日始使用新利息，2表示下一付息期使用新利息，3表示调息日对应日，4表示本存续期间，99表示无，若无，则按照2方式处理。
        "001": 1,
        "002": 4,
        "003": 2,
        "005": 3,
        "999": 2
    }

    calc_way_mapping = {
        # 保留整数。001为平均分配，002为按实际天数计息。若为9，则为未披露，此时采用原处理规则进行处理
        # cpp 保留整数。1为按实际天数计息，2为平均分配。若为9，则为未披露，此时采用原处理规则进行处理
        "001": 2,
        "002": 1,
        "999": 9
    }


if __name__ == '__main__':
    BondInfo.run_etl(is_init=True)
