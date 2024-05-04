# vim set fileencoding=utf-8

import json
from datetime import date, datetime
from typing import Optional, Union, List
from math import log
import pyarrow as pa

from qt_common.qt_logging import frame_log as logger
from qt_common.utils import date_to_str
from qt_etl.entity.fields import Dimension, Measure
from qt_etl.entity.market_data.market_data import MarketData
from qt_etl.entity.portfolio.comb_position import resource_decorator

__all__ = ["StockDailyQuote"]


class StockDailyQuote(MarketData):
    """股票市场数据"""
    party_code_source_table = {
        "INFO_STK_EODPRICE": "PARTY_CODE",
        "INFO_PARTY_SHRSTRUC": "PARTY_CODE",
    }
    end_date_source_table = {
        "INFO_STK_EODPRICE": "END_DATE",
        "INFO_PARTY_SHRSTRUC": "ANC_DATE",
    }
    schema = pa.schema([
        pa.field(Dimension.ISSUER_CODE, pa.string(),
                 metadata={b'source_table': json.dumps(party_code_source_table).encode()}),
        pa.field(Measure.CLOSE_ADJUSTED, pa.float64(),
                 metadata={b'table_field': b'CLS_PRC_ADJ', b'table_name': b'INFO_STK_EODPRICE'}),
        pa.field(Measure.PREV_CLOSE_ADJUSTED, pa.float64(),
                 metadata={b'table_field': b'PRE_CLS_PRC_ADJ', b'table_name': b'INFO_STK_EODPRICE'}),
        pa.field(Measure.RETURN_PERCENTAGE, pa.float64(),
                 metadata={b'table_field': b'PCT_CHG', b'table_name': b'INFO_STK_EODPRICE'}),
        pa.field(Dimension.TRADE_DATE, pa.string(),
                 metadata={b'source_table': json.dumps(end_date_source_table).encode()}),
        pa.field(Dimension.INSTRUMENT_CODE, pa.string(),
                 metadata={b'table_field': b'STK_CODE', b'table_name': b'INFO_STK_VALUATION'}),
        pa.field(Measure.TRADE_AMOUNT, pa.float64(),
                 metadata={b'table_field': b'AMT', b'table_name': b'INFO_STK_VALUATION'}),
        pa.field(Measure.VOLUME, pa.float64(),
                 metadata={b'table_field': b'VOL', b'table_name': b'INFO_STK_VALUATION'}),
        pa.field(Measure.HIGH, pa.float64(),
                 metadata={b'table_field': b'HI_PRC', b'table_name': b'INFO_STK_VALUATION'}),
        pa.field(Measure.LOW, pa.float64(),
                 metadata={b'table_field': b'LO_PRC', b'table_name': b'INFO_STK_VALUATION'}),
        pa.field(Measure.TURNOVER, pa.float64(),
                 metadata={b'table_field': b'TURN_RAT', b'table_name': b'INFO_STK_VALUATION'}),
        pa.field(Measure.CLOSE, pa.float64(),
                 metadata={b'table_field': b'CLS_PRC', b'table_name': b'INFO_STK_VALUATION'}),
        pa.field(Measure.PREV_CLOSE, pa.float64(),
                 metadata={b'table_field': b'PRE_CLS_PRC', b'table_name': b'INFO_STK_VALUATION'}),
        pa.field(Measure.NEGOTIABLE_MARKET_CAPITALIZATION, pa.float64(),
                 metadata={b'table_field': b'CV', b'table_name': b'INFO_STK_VALUATION'}),
        pa.field(Measure.MARKET_CAPITALIZATION, pa.float64(),
                 metadata={b'table_field': b'MV', b'table_name': b'INFO_STK_VALUATION'}),
        pa.field(Measure.DIVIDEND_YIELD, pa.float64(),
                 metadata={b'table_field': b'DVD_YLD', b'table_name': b'INFO_STK_VALUATION'}),
        pa.field(Measure.PREFERRED_STOCK, pa.float64(),
                 metadata={b'table_field': b'SHR_PS', b'table_name': b'INFO_PARTY_SHRSTRUC'}),
        pa.field(Dimension.DUE_DATE, pa.string(),
                 metadata={b'table_field': b'CHG_DATE', b'table_name': b'INFO_PARTY_SHRSTRUC'}),
        pa.field(Measure.SHARES_OUTSTANDING, pa.float64(),
                 metadata={b'table_field': b'SHR_TTL', b'table_name': b'INFO_PARTY_SHRSTRUC'}),
        pa.field(Measure.CIRCULATING_SHARES, pa.float64(),
                 metadata={b'table_field': b'SHR_CRC', b'table_name': b'INFO_PARTY_SHRSTRUC'}),
        pa.field(Measure.RETURN_PERCENTAGE_ADJUSTED, pa.float64(),
                 metadata={b'table_field': b'PCT_CHG', b'table_name': b'INFO_STK_EODPRICE'}),
        pa.field(Measure.RETURN_PERCENTAGE_ADJUSTED_LOG, pa.float64(),
                 metadata={b'table_field': b'PCT_CHG', b'table_name': b'INFO_STK_EODPRICE'}),
    ],
        metadata={
            Dimension.INSTRUMENT_CODE: '股票内部编码',
            Dimension.ISSUER_CODE: '公司代码',
            Dimension.TRADE_DATE: 'trade_date',
            Dimension.DUE_DATE: '截止日期',
            Measure.CLOSE_ADJUSTED: '字段来源-复权收盘价（元）',
            Measure.PREV_CLOSE_ADJUSTED: '',
            Measure.RETURN_PERCENTAGE: '字段来源-涨跌幅（%）',
            Measure.TRADE_AMOUNT: '字段来源-成交金额（元）', # changed from Measure.TURNOVER
            Measure.VOLUME: '字段来源-成交量（股）', # changed from Measure.TRADE_AMOUNT
            Measure.CLOSE: '字段来源-收盘价（元）',
            Measure.PREV_CLOSE: '字段来源-昨收盘（元）',
            Measure.HIGH: '字段来源-最高价（元）',
            Measure.LOW: '字段来源-最低价（元）',
            Measure.TURNOVER: '字段来源-换手率（%）',
            Measure.NEGOTIABLE_MARKET_CAPITALIZATION: '',
            Measure.MARKET_CAPITALIZATION: '',
            Measure.DIVIDEND_YIELD: '',
            Measure.PREFERRED_STOCK: '优先股',
            Measure.SHARES_OUTSTANDING: '总股本(股)',
            Measure.CIRCULATING_SHARES: '流通股',
            Measure.RETURN_PERCENTAGE_ADJUSTED: '复权涨跌幅',
            Measure.RETURN_PERCENTAGE_ADJUSTED_LOG: '复权对数涨跌幅',
        }
    )

    @classmethod
    def fetch_data(cls,
                   secu_code: str = None,
                   start_date: Optional[Union[datetime, date]] = None,
                   end_date: Optional[Union[datetime, date]] = None):
        df_structure_change = cls.etl_stock_structure_change_market_data(start_date, end_date)
        logger.info(f'length of stock structure change is {len(df_structure_change)}')
        # 判断是否为空
        if df_structure_change.empty:
            return df_structure_change
        df_stock = cls.etl_stock_market_data_query(start_date, end_date, secu_code=secu_code)
        logger.info(f'length of stock market data is {len(df_stock)}')
        if df_stock.empty:
            return df_stock

        df = df_stock.merge(df_structure_change, how='left', left_on=['PARTY_CODE', 'END_DATE'],
                            right_on=['PARTY_CODE', 'ANC_DATE'])
        df = df.drop(columns=['ANC_DATE'])
        df = df.rename(columns=cls.etl_rename_dict)
        # TODO: 如果某只股票最早的股本数据为空，此时填充会出现数据错误。或者不进行填充
        # 要填充的字段
        filled_column = [Measure.PREFERRED_STOCK, Dimension.DUE_DATE, Measure.SHARES_OUTSTANDING,
                         Measure.CIRCULATING_SHARES]
        df.sort_values(by=[Dimension.INSTRUMENT_CODE, Dimension.TRADE_DATE], ascending=True, inplace=True)
        # 向下填充
        df[filled_column] = df[filled_column].fillna(method='ffill')
        # todo 如果第一个行为空，向下填充还是可能为空
        if df[filled_column].isnull().any().any():
            df[filled_column] = df[filled_column].fillna(method='bfill')
        df = df.astype(cls.type_dict)
        logger.info(f'length of stock market data after merge with structure change is {len(df_stock)}')
        df[Measure.RETURN_PERCENTAGE] /= 100
        df[Measure.DIVIDEND_YIELD] /= 100
        df[Measure.RETURN_PERCENTAGE_ADJUSTED] = df.apply(
            lambda row: row[Measure.CLOSE_ADJUSTED] / row[Measure.PREV_CLOSE_ADJUSTED] - 1 if (
                    row[Measure.CLOSE_ADJUSTED] and row[Measure.PREV_CLOSE_ADJUSTED]) else None, axis=1)
        # 都为null时，存文件会报错
        df[Measure.RETURN_PERCENTAGE_ADJUSTED] = df[Measure.RETURN_PERCENTAGE_ADJUSTED].astype(float)
        df[Measure.RETURN_PERCENTAGE_ADJUSTED_LOG] = df[Measure.RETURN_PERCENTAGE_ADJUSTED].apply(
            lambda x: log(x + 1) if x is not None else None)
        df[Measure.RETURN_PERCENTAGE_ADJUSTED_LOG] = df[Measure.RETURN_PERCENTAGE_ADJUSTED_LOG].astype(float)

        df[Dimension.TRADE_DATE] = df[Dimension.TRADE_DATE].apply(date_to_str)
        df[Dimension.DUE_DATE] = df[Dimension.DUE_DATE].apply(date_to_str)
        df[Dimension.TRADE_DATE] = df[Dimension.TRADE_DATE].str.lower()
        df = df.reset_index(drop=True)
        return df

    # TODO what is DUE_DATE
    @classmethod
    def etl_stock_structure_change_market_data(cls,
                                               start_date: Optional[Union[datetime, date]] = None,
                                               end_date: Optional[Union[datetime, date]] = None):
        sql = f"""	SELECT
            INFO_PARTY_SHRSTRUC.PARTY_CODE,
            INFO_PARTY_SHRSTRUC.ANC_DATE,
            INFO_PARTY_SHRSTRUC.SHR_PS,
            INFO_PARTY_SHRSTRUC.CHG_DATE,
            INFO_PARTY_SHRSTRUC.SHR_TTL,
            INFO_PARTY_SHRSTRUC.SHR_CRC
        FROM
            INFO_PARTY_SHRSTRUC
        WHERE
            ANC_DATE BETWEEN '{start_date}' AND '{end_date}'
        """
        #             --INFO_STK_BASICINFO.STK_CODE as {cls.INSTRUMENT_CODE}
        # --LEFT join STK_BASICINFO on INFO_STK_BASICINFO.PARTY_CODE=INFO_PARTY_SHRSTRUC.PARTY_CODE
        df = cls.query(sql)
        logger.info(f'structure_change len:{len(df)}')
        # 去重，保留最新的一条 CHG_DATE最晚的一条
        res_df = df.drop_duplicates(subset=['PARTY_CODE', 'ANC_DATE'], keep='last')
        # logger.info(f'structure_change 去重后 len:{len(res_df)}')
        return res_df

    @classmethod
    def etl_stock_market_data_query(cls,
                                    start_date: Optional[Union[datetime, date]] = None,
                                    end_date: Optional[Union[datetime, date]] = None,
                                    secu_code: Optional[Union[str, List[str]]] = None):
        # 增加基准指数成分股
        sql_idx_sec_codes = f"""
                            select CPN_CODE from INFO_IDX_WT_STK where IDX_CODE ='SEC024342013' and TRD_DATE BETWEEN '{start_date}' AND '{end_date}' group by CPN_CODE
                """
        idx_sec_codes_list = list(cls.query(sql_idx_sec_codes)['CPN_CODE'])
        if idx_sec_codes_list:
            idx_sec_codes = "{}".format((",".join([f"'{k}'" for k in idx_sec_codes_list])))
            if secu_code:
                secu_code += ',' + idx_sec_codes
            else:
                secu_code = idx_sec_codes
        sql = f"""SELECT
                INFO_STK_BASICINFO.PARTY_CODE,
                INFO_STK_EODPRICE.CLS_PRC_ADJ,
                INFO_STK_EODPRICE.PRE_CLS_PRC_ADJ,
                INFO_STK_EODPRICE.PCT_CHG,
                INFO_STK_EODPRICE.END_DATE,
                INFO_STK_EODPRICE.STK_CODE,
                INFO_STK_EODPRICE.AMT,
                INFO_STK_EODPRICE.VOL,
                INFO_STK_EODPRICE.HI_PRC,
                INFO_STK_EODPRICE.LO_PRC,
                INFO_STK_EODPRICE.TURN_RAT,
                INFO_STK_EODPRICE.CLS_PRC,
                INFO_STK_EODPRICE.PRE_CLS_PRC,
                INFO_STK_VALUATION.CV,
                INFO_STK_VALUATION.MV,
                INFO_STK_VALUATION.DVD_YLD
            FROM
                INFO_STK_EODPRICE
            INNER JOIN INFO_STK_VALUATION ON
                INFO_STK_EODPRICE.STK_CODE = INFO_STK_VALUATION.STK_CODE
                AND INFO_STK_EODPRICE.END_DATE = INFO_STK_VALUATION.END_DATE
            LEFT JOIN INFO_STK_BASICINFO ON 
                INFO_STK_EODPRICE.STK_CODE = INFO_STK_BASICINFO.STK_CODE
            WHERE
                INFO_STK_EODPRICE.END_DATE BETWEEN '{start_date}' AND '{end_date}' 
            """
        if secu_code:
            sql += f" AND INFO_STK_EODPRICE.STK_CODE in ({secu_code})"
        df = cls.query(sql)
        return df

    etl_rename_dict = {
        'STK_CODE': Dimension.INSTRUMENT_CODE,
        'PARTY_CODE': Dimension.ISSUER_CODE,
        'END_DATE': Dimension.TRADE_DATE,
        'CHG_DATE': Dimension.DUE_DATE,
        'CLS_PRC_ADJ': Measure.CLOSE_ADJUSTED,
        'PRE_CLS_PRC_ADJ': Measure.PREV_CLOSE_ADJUSTED,
        'PCT_CHG': Measure.RETURN_PERCENTAGE,
        'AMT': Measure.TRADE_AMOUNT,
        'VOL': Measure.VOLUME,
        'HI_PRC': Measure.HIGH,
        'LO_PRC': Measure.LOW,
        'TURN_RAT': Measure.TURNOVER,
        'CLS_PRC': Measure.CLOSE,
        'PRE_CLS_PRC': Measure.PREV_CLOSE,
        'CV': Measure.NEGOTIABLE_MARKET_CAPITALIZATION,
        'MV': Measure.MARKET_CAPITALIZATION,
        'DVD_YLD': Measure.DIVIDEND_YIELD,
        'SHR_PS': Measure.PREFERRED_STOCK,
        'SHR_TTL': Measure.SHARES_OUTSTANDING,
        'SHR_CRC': Measure.CIRCULATING_SHARES,
    }

    type_dict = {
        Measure.PREV_CLOSE_ADJUSTED: float,
        Measure.RETURN_PERCENTAGE: float,
        Measure.CLOSE_ADJUSTED: float,
        Measure.TURNOVER: float,
        Measure.TRADE_AMOUNT: float,
        Measure.HIGH: float,
        Measure.LOW: float,
        Measure.CLOSE: float,
        Measure.PREV_CLOSE: float,
        Measure.NEGOTIABLE_MARKET_CAPITALIZATION: float,
        Measure.MARKET_CAPITALIZATION: float,
        Measure.DIVIDEND_YIELD: float,
        Measure.PREFERRED_STOCK: float,
        Measure.SHARES_OUTSTANDING: float,
        Measure.CIRCULATING_SHARES: float
    }

    @classmethod
    @resource_decorator(secu_type="STOCK")
    def run_etl(cls, secu_codes: Optional[list[str]] = None,
                start_date: Optional[Union[datetime, date]] = None,
                end_date: Optional[Union[datetime, date]] = date.today(),
                is_concurrent_query=True,
                is_concurrent_save=False,
                is_init=False):
        super(StockDailyQuote, cls).run_etl(
            secu_codes=secu_codes,
            start_date=start_date,
            end_date=end_date,
            is_concurrent_save=is_concurrent_save,
            is_concurrent_query=is_concurrent_query,
            is_init=is_init)


if __name__ == '__main__':
    StockDailyQuote.run_etl()
