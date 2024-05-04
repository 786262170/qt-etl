# vim set fileencoding=utf-8
from datetime import datetime, date
from typing import Optional, Union

import pyarrow as pa

from qt_common.utils import date_to_str
from qt_etl.entity.fields import Dimension, Measure
from qt_etl.entity.portfolio.portfolio import Portfolio

__all__ = ["CombAsset"]


class CombAsset(Portfolio):
    """组合资产集市数据"""
    schema = pa.schema([
        pa.field(Dimension.INSTRUMENT_CODE, pa.string(), metadata={b"table_field": b"PRODUCT_CODE"}),
        pa.field(Dimension.TRADE_DATE, pa.string(), metadata={b"table_field": b"BUSI_DATE"}),
        pa.field(Measure.CLOSE, pa.float64(), metadata={b"table_field": b"PE_NET_ASSET"}),
        pa.field(Measure.QUANTITY, pa.float64(), metadata={b"table_field": b"PE_SHARES"}),
        pa.field(Measure.NET_CPTL_INFLOW_AMT, pa.float64(), metadata={b"table_field": b"NET_CPTL_INFLOW_AMT"}),
        pa.field(Measure.LEVERAGE_RATE, pa.float64(), metadata={b"table_field": b"LEVERAGE_RATE"}),
        pa.field(Measure.CASH_RATIO, pa.float64(), metadata={b"table_field": b"CASH_RATIO"}),
        pa.field(Measure.INST_POSI_RIN, pa.float64(), metadata={b"table_field": b"INST_POSI_RIN"}),
        pa.field(Measure.REALIZABLE_MARKET_VALUE, pa.float64(), metadata={b"table_field": b"ST_REALIZABLE_MKTVAL"}),
        pa.field(Measure.MARKET_VALUE, pa.float64(), metadata={b"table_field": b"MKTV_FUND"}),
        pa.field(Dimension.DOWN_FLAG, pa.string(), metadata={b"table_field": b"DOWN_FLAG"}),
    ],
        metadata={
            Dimension.INSTRUMENT_CODE: '产品代码',
            Dimension.TRADE_DATE: '业务日期',
            Measure.CLOSE: '净资产_期末',
            Measure.QUANTITY: '份额_期末',
            Measure.NET_CPTL_INFLOW_AMT: '组合净资金流入金额',
            Measure.LEVERAGE_RATE: '杠杆率',
            Measure.CASH_RATIO: '现金比例',
            Measure.INST_POSI_RIN: '机构持仓占比',
            Measure.RETURN_PERCENTAGE: '组合短期可变现资产市值',
            Measure.MARKET_VALUE: '基金投资市值',
            Dimension.DOWN_FLAG: '是否穿透',
        }
    )

    etl_rename_dict = {
        'PRODUCT_CODE': Dimension.INSTRUMENT_CODE,
        'BUSI_DATE': Dimension.TRADE_DATE,
        'PE_NET_ASSET': Measure.CLOSE,
        'PE_SHARES': Measure.QUANTITY,
        'NET_CPTL_INFLOW_AMT': Measure.NET_CPTL_INFLOW_AMT,
        'LEVERAGE_RATE': Measure.LEVERAGE_RATE,
        'CASH_RATIO': Measure.CASH_RATIO,
        'INST_POSI_RIN': Measure.INST_POSI_RIN,
        'ST_REALIZABLE_MKTVAL': Measure.REALIZABLE_MARKET_VALUE,
        'MKTV_FUND': Measure.MARKET_VALUE,
        'DOWN_FLAG': Dimension.DOWN_FLAG
    }

    @classmethod
    def fetch_data(cls,
                   secu_code: str = None,
                   start_date: Optional[Union[datetime, date]] = None,
                   end_date: Optional[Union[datetime, date]] = None):
        sql = f"""select
            PE_NET_ASSET,
            PE_SHARES,
            NET_CPTL_INFLOW_AMT,
            PRODUCT_CODE ,
            BUSI_DATE,
            LEVERAGE_RATE,
            CASH_RATIO,
            INST_POSI_RIN,
            ST_REALIZABLE_MKTVAL,
            MKTV_FUND,
            DOWN_FLAG
        from
            DM_PORT_ASSET"""
        df = cls.query(sql)
        if not df.empty:
            df = df.rename(columns=cls.etl_rename_dict)
            df[Dimension.TRADE_DATE] = df[Dimension.TRADE_DATE].apply(date_to_str).astype(str)
        return df


if __name__ == '__main__':
    CombAsset.run_etl(start_date=date(2021, 1, 20), end_date=date(2022, 2, 20))

    df = CombAsset.get_data()
    print(df)
