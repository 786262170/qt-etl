from datetime import date, datetime
from typing import Optional, Union

import numpy as np
import pandas as pd

from qt_common.utils import date_to_str
from qt_etl.entity.market_data import StockDailyQuote, FundDailyQuote
from qt_etl.entity.market_data.market_data import MarketData
from qt_etl.entity.market_data.yield_curve_cnbd_sample import curve_code_decorator
from qt_etl.entity.fields import Dimension, Measure, InstrumentType, Currency, TimeSeriesType


class TimeSeries(MarketData):
    """曲线插值"""
    model_dict = {
        StockDailyQuote.__name__: StockDailyQuote,
        FundDailyQuote.__name__: FundDailyQuote
    }

    @classmethod
    def fetch_data(cls,
                   secu_codes: str = None,
                   start_date: Optional[Union[datetime, date]] = None,
                   end_date: Optional[Union[datetime, date]] = None
                   ):
        if secu_codes and not isinstance(secu_codes, str):
            raise Exception('TimeSeries secu_codes参数有误')
        df1 = cls.get_model_time_series(StockDailyQuote.__name__, start_date=start_date, end_date=end_date)
        df2 = cls.get_model_time_series(FundDailyQuote.__name__, start_date=start_date, end_date=end_date)
        df3 = cls.get_curve_time_series(start_date, end_date, secu_codes)
        df_cfet_curve = cls.get_cfet_yield_curve(start_date, end_date, secu_codes)
        df_deposit_loan_rate = cls.fetch_deposit_loan_rate(secu_codes, start_date, end_date)
        df_ibor_rate = cls.fetch_global_ibor_rate(secu_codes, start_date, end_date)
        df_mm_quo = cls.fetch_MM_QUO(secu_codes, start_date, end_date)
        curve_df = pd.concat([df_cfet_curve, df_deposit_loan_rate, df_ibor_rate, df_mm_quo])
        if not curve_df.empty:
            common_columns = curve_df.columns.tolist()
            common_columns.remove(Dimension.TRADE_DATE)
            common_columns.remove(Dimension.RATE)
            curve_df = curve_df.pivot(index=common_columns, columns=Dimension.TRADE_DATE,
                                      values=Dimension.RATE).reset_index()
        df = pd.concat([df1, df2, df3, curve_df])
        return df

    @classmethod
    def get_curve_time_series(cls, start_date, end_date, curve_codes):
        # 插值
        sql = f"""SELECT CURV_CODE,
                         TRD_DATE,
                         STD_TERM,
                         YIELD
                    FROM INFO_FI_CNBD_YIELD_CURV, INFO_FI_IR_BASICINFO
                    WHERE INFO_FI_CNBD_YIELD_CURV.CURV_CODE=INFO_FI_IR_BASICINFO.IRCODE
                    and TRD_DATE between '{start_date}' and '{end_date}'
                    and INFO_FI_IR_BASICINFO.IRTYPE='023'
                    and CURV_CODE in ({curve_codes})
                    """
        df = cls.query(sql)
        if not df.empty:
            df = df.rename(columns={'CURV_CODE': Dimension.INDEX,
                                    'TRD_DATE': Dimension.TRADE_DATE,
                                    'STD_TERM': Dimension.TENOR,
                                    'YIELD': Measure.RATE})
            df = df[~df[Measure.RATE].isna()]
            df = df.sort_values(by=[Dimension.INDEX, Dimension.TRADE_DATE, Dimension.TENOR])
            df[Measure.RATE] = df[Measure.RATE].astype(float) / 100
            df[Dimension.TRADE_DATE] = df[Dimension.TRADE_DATE].apply(date_to_str)
            df[Dimension.TENOR] = df[Dimension.TENOR].astype(float)
            df_interperted = pd.DataFrame()
            df_interperted = df_interperted.dropna(how='all')
            for idx, idx_data in df.groupby(Dimension.INDEX):
                tenor_list = [round(i / 365, 2) for i in range(int(max(idx_data[Dimension.TENOR]) * 365 + 1))]
                index_interp_data = pd.DataFrame(set(tenor_list), columns=[Dimension.TENOR])
                index_interp_data[Dimension.INDEX] = idx
                for trd_date, index_data_day in idx_data.groupby(Dimension.TRADE_DATE):
                    interp_index_data_day = pd.Series(np.interp(index_interp_data[Dimension.TENOR],
                                                                index_data_day[Dimension.TENOR],
                                                                index_data_day[Dimension.RATE]), name=trd_date)
                    index_interp_data = pd.concat([index_interp_data, interp_index_data_day], axis=1)
                df_interperted = pd.concat([df_interperted, index_interp_data]).reset_index(drop=True)
            df_interperted[Dimension.TENOR] = df_interperted[Dimension.TENOR].astype(str) + 'Y'
            df_interperted[Dimension.TIME_SERIES_TYPE] = TimeSeriesType.YIELD_CURVE.name
            return df_interperted
        else:
            return pd.DataFrame()

    @classmethod
    def get_cfet_yield_curve(cls,
                             start_date, end_date, curve_codes):
        sql = f"""SELECT
            TRD_DATE,
            STD_TERM,
            YIELD,
            CURV_CODE
        FROM
            INFO_FI_CFETS_YIELD_CURV
        WHERE
            TRD_DATE between '{start_date}' and '{end_date}'
            and CURV_CODE in ("CFE100432", "CFE100552", "CFE100372", "CFE101562")"""
        df = cls.query(sql)
        if not df.empty:
            df = df.rename(columns={'CURV_CODE': Dimension.INDEX,
                                    'TRD_DATE': Dimension.TRADE_DATE,
                                    'STD_TERM': Dimension.TENOR,
                                    'YIELD': Measure.RATE})
            df = df[~df[Measure.RATE].isna()]
            df[Dimension.CURRENCY] = str(Currency.CNY).lower()
            df[Dimension.INSTRUMENT_TYPE] = InstrumentType.DEPOSIT.name
            df[Dimension.TRADE_DATE] = df[Dimension.TRADE_DATE].apply(date_to_str).astype(str)
            df[Dimension.TENOR] = df[Dimension.TENOR].apply(
                lambda x: f'{int(x)}Y' if x.is_integer() else cls.irs_curve_tenor_mapping.get(str(x)))
            df[Measure.RATE] = df[Measure.RATE].astype(float) / 100
            df[Dimension.TIME_SERIES_TYPE] = TimeSeriesType.IRS_CURVE.name
            df[Dimension.SYMBOL] = df[Dimension.INDEX].map(cls.irs_curve_name_mapping)
        return df

    @classmethod
    def transform(cls, df, type):
        transform_colums = Measure.CLOSE_ADJUSTED if type == TimeSeriesType.ADJUSTED_PRICE.name else Measure.RETURN_PERCENTAGE_ADJUSTED
        columns = [Dimension.INSTRUMENT_CODE, Dimension.TRADE_DATE] + [transform_colums]
        df = df[columns]
        df = df.rename(columns={Dimension.INSTRUMENT_CODE: Dimension.INDEX})
        # 行转列
        df = df.pivot(index=Dimension.INDEX, columns=Dimension.TRADE_DATE, values=transform_colums).reset_index()
        df[Dimension.TIME_SERIES_TYPE] = type
        df[Dimension.TENOR] = ''
        return df

    @classmethod
    def get_model_time_series(cls, model_name, secu_codes=None, start_date=None, end_date=None):
        if model_name not in cls.model_dict:
            raise Exception(f'get_model_time_series 暂不支持model:{model_name}')
        model = cls.model_dict.get(model_name)
        parms = {}
        if secu_codes is not None:
            parms['secu_codes'] = secu_codes
        if start_date is not None:
            parms['start_date'] = start_date
        if end_date is not None:
            parms['end_date'] = end_date
        df = model.get_data(**parms)
        if not df.empty:
            df_price = cls.transform(df, TimeSeriesType.ADJUSTED_PRICE.name)
            df_return = cls.transform(df, TimeSeriesType.ADJUSTED_RETURN.name)
            df = pd.concat([df_return, df_price])
        return df

    @classmethod
    def fetch_deposit_loan_rate(cls,
                                secu_codes: str = None,
                                start_date: Optional[Union[datetime, date]] = None,
                                end_date: Optional[Union[datetime, date]] = None
                                ):
        sql = f"""SELECT
                            IR_CODE,
                            CNAME,
                            CHG_START_DATE,
                            INTR_RAT
                      FROM
                            INFO_FI_BNK_DL_IR
                      WHERE CHG_START_DATE between '{start_date}' and '{end_date}'"""
        df = cls.query(sql)
        if not df.empty:
            df = df.rename(columns={
                'IR_CODE': Dimension.INDEX,
                'CNAME': Dimension.INDEX_NAME,
                'CHG_START_DATE': Dimension.TRADE_DATE,
                'INTR_RAT': Measure.RATE
            })
            df = df[~df[Dimension.TRADE_DATE].isna()]
            df[Dimension.TRADE_DATE] = df[Dimension.TRADE_DATE].apply(date_to_str)
            df[Dimension.CURRENCY] = str(Currency.CNY).lower()
            df[Dimension.INSTRUMENT_TYPE] = InstrumentType.DEPOSIT.name
            df[Measure.RATE] = df[Measure.RATE].astype(float) / 100
            df[Dimension.TIME_SERIES_TYPE] = TimeSeriesType.DEPOSIT.name
        return df

    @classmethod
    def fetch_global_ibor_rate(cls,
                               secu_codes: str = None,
                               start_date: Optional[Union[datetime, date]] = None,
                               end_date: Optional[Union[datetime, date]] = None
                               ):

        sql = f"""select
                        IR_CODE,
                        b.CNAME,
                        SYMBOL,
                        TRD_DATE,
                        INTR_RAT
                    from
                        INFO_FI_GLOBAL_IBOR a,
                        INFO_FI_IR_BASICINFO b
                    WHERE TRD_DATE between '{start_date}' and '{end_date}'
                    and a.IR_CODE=b.IRCODE"""
        df = cls.query(sql)
        if not df.empty:
            df = df.rename(columns={
                'IR_CODE': Dimension.INDEX,
                'CNAME': Dimension.INDEX_NAME,
                'TRD_DATE': Dimension.TRADE_DATE,
                'INTR_RAT': Measure.RATE,
                'SYMBOL': Dimension.SYMBOL
            })
            df = df[~df[Dimension.TRADE_DATE].isna()]
            df[Dimension.TRADE_DATE] = df[Dimension.TRADE_DATE].apply(date_to_str)
            df[Dimension.CURRENCY] = str(Currency.CNY).lower()
            df[Dimension.INSTRUMENT_TYPE] = InstrumentType.DEPOSIT.name
            df[Measure.RATE] = df[Measure.RATE].astype(float) / 100
            df[Dimension.TENOR] = df[Dimension.INDEX].map(cls.ibor_tenor_mapping)
            df[Dimension.TIME_SERIES_TYPE] = TimeSeriesType.DEPOSIT.name
        return df

    @classmethod
    def fetch_MM_QUO(cls,
                     secu_codes: str = None,
                     start_date: Optional[Union[datetime, date]] = None,
                     end_date: Optional[Union[datetime, date]] = None
                     ):
        # FR007 银行间7天回购定盘利率
        sql = f"""select
                    IR_CODE,
                    SYMBOL,
                    CNAME,
                    TRD_DATE,
                    CLS_QUO_IR
                from
                    INFO_FI_MM_QUO,
                    INFO_FI_IR_BASICINFO
                where
                    INFO_FI_MM_QUO.IR_CODE = INFO_FI_IR_BASICINFO.IRCODE
                    and TRD_DATE between '{start_date}' and '{end_date}'
                    and IR_CODE='FR001001'
                    """
        df = cls.query(sql)
        if not df.empty:
            df = df.rename(columns={
                'IR_CODE': Dimension.INDEX,
                'CNAME': Dimension.INDEX_NAME,
                'TRD_DATE': Dimension.TRADE_DATE,
                'CLS_QUO_IR': Measure.RATE,
                'SYMBOL': Dimension.SYMBOL
            })
            df = df[~df[Dimension.TRADE_DATE].isna()]
            df[Dimension.TRADE_DATE] = df[Dimension.TRADE_DATE].apply(date_to_str)
            df[Dimension.CURRENCY] = str(Currency.CNY).lower()
            df[Dimension.INSTRUMENT_TYPE] = InstrumentType.DEPOSIT.name
            df[Measure.RATE] = df[Measure.RATE].astype(float) / 100
            df[Dimension.TENOR] = df[Dimension.INDEX].map(cls.mm_quo_tenor_mapping)
            df[Dimension.TIME_SERIES_TYPE] = TimeSeriesType.DEPOSIT.name
        return df

    @classmethod
    def fetch_curve_type(cls):
        sql = """select
                INFO_FI_IR_BASICINFO.IRCODE, 
                INFO_FI_IR_BASICINFO.IRTYPE,
                INFO_PARA_INFO.PARA_NAME
            from
                INFO_FI_IR_BASICINFO
            left join INFO_PARA_INFO
            on
                INFO_FI_IR_BASICINFO.IRTYPE = INFO_PARA_INFO.para_code
                and INFO_PARA_INFO.para_obj = '031001'"""

        df = cls.query(sql)
        if not df.empty:
            df = df.rename(columns={
                "IRCODE": Dimension.INDEX,
                "IRTYPE": Dimension.CURVE_TYPE,
                "PARA_NAME": Dimension.CURVE_TYPE_NAME
            })
        return df

    ibor_tenor_mapping = {
        'CN002001': '5Y',
        'SH001001': '1D',
        'SH001002': '1W',
        'SH001003': '2W',
        'SH001004': '1M',
        'SH001005': '3M',
        'SH001006': '6M',
        'SH001007': '9M',
        'SH001008': '1Y',
        'SH002001': '1Y'
    }

    irs_curve_name_mapping = {
        'CFE100372': 'SHIB3M',
        'CFE100432': 'SHIBON',
        'CFE100552': 'FR007',
        'CFE101562': 'FDR007',
    }

    irs_curve_tenor_mapping = {
        '0.0833': "1M",
        '0.25': "3M",
        '0.5': "6M",
        '0.75': "9M"
    }

    bank_dl_tenor_mapping = {
        'DL001006': '1Y'
    }

    mm_quo_tenor_mapping = {
        'FR001001': '7D'  # 7d
    }

    @classmethod
    def get_shockable_factors(cls):
        df = cls.get_data()
        df = df[~df[Dimension.TENOR].isna()]
        df = df[[Dimension.INDEX, Dimension.INDEX_NAME, Dimension.TENOR,
                 Dimension.INSTRUMENT_TYPE]].drop_duplicates()
        return df

    @classmethod
    @curve_code_decorator
    def run_etl(cls, secu_codes: Optional[list[str]] = None,
                start_date: Optional[Union[datetime, date]] = None,
                end_date: Optional[Union[datetime, date]] = date.today(),
                is_concurrent_query=False,
                is_concurrent_save=False,
                is_init=False):
        return super(TimeSeries, cls).run_etl(
            secu_codes=secu_codes,
            start_date=start_date,
            end_date=end_date,
            is_concurrent_query=is_concurrent_query,
            is_concurrent_save=is_concurrent_save,
            is_init=is_init)


if __name__ == '__main__':
    index_list = ['CBD100222', 'SEC022392986', 'SEC024340484', 'SEC024277699', 'SEC024272388', 'SEC024269457',
                  'SEC024214507', 'SEC024168222', 'SEC024167571', 'SEC024116015', 'SEC024111578', 'SEC024105631',
                  'SEC024040891', 'SEC024025018', 'SEC023977909', 'SEC023972269', 'SEC023911177', 'SEC023886355',
                  'SEC024329142', 'SEC024340673', 'SEC022418694', 'SEC024343208', 'SEC024831698', 'SEC024822966',
                  'SEC024816307', 'SEC024789806', 'SEC024782148', 'SEC024757663', 'SEC024684465', 'SEC024678359',
                  'SEC024635899', 'SEC024403333', 'SEC024391024', 'SEC024383719', 'SEC024377827', 'SEC024351769',
                  'SEC024349051', 'SEC023816915', 'SEC023794969', 'SEC023794581', 'SEC023788904', 'SEC022717458',
                  'SEC022710546', 'SEC022710534', 'SEC022689625', 'SEC022671518', 'SEC022627958', 'SEC022610801',
                  'SEC022590900', 'SEC022583959', 'SEC022580137', 'SEC022508608', 'SEC022491314', 'SEC022491014',
                  'SEC022445441', 'SEC022422182', 'SEC022733789', 'SEC022827426', 'SEC022915500', 'SEC023441591',
                  'SEC023769570', 'SEC023735374', 'SEC023735372', 'SEC023730153', 'SEC023700855', 'SEC023487327',
                  'SEC023434247', 'SEC023036504', 'SEC023385032', 'SEC023374901', 'SEC023289898', 'SEC023236414',
                  'SEC023135662', 'SEC023118876', 'SEC024846974', 'SEC023706777', 'SEC023974796', 'SEC023655827',
                  'SEC024205912', 'SEC023905600', 'SEC024271109', 'SEC026331662', 'SEC024388067', 'SEC024313981']
    TimeSeries.run_etl(start_date=date(2021, 1, 1), end_date=date(2022, 12, 31), secu_codes=index_list)
    df = TimeSeries.get_data(cond={Dimension.SYMBOL: 'FR007'})
    print(df)
