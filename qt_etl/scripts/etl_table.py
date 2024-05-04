"""
获取表字段注释
"""
import os.path

import pandas as pd

from qt_common.db_manager import pd_read_sql
from qt_etl.config import settings

financial_data = [
    {
        "model": "FinancialIndicator",
        "table": "INFO_PARTY_FIN_BALANCE",
        "columns": ["PARTY_CODE",
                    "RPT_TYPE",
                    "RPT_TERM_TYPE",
                    "ANC_DATE",
                    "END_DATE",
                    "LIAB_NONCUR",
                    "LIAB_TTL",
                    "AST_TTL",
                    "MNY_FUND",
                    "FIN_AST_TRAS",
                    "RCV_NOTE",
                    "OE_TTL",
                    "OE_MINOR"]
    },
    {
        "model": "FinancialIndicator",
        "table": "INFO_PARTY_FIN_INCOME",
        "columns": ["PARTY_CODE",
                    "RPT_TYPE",
                    "RPT_TERM_TYPE",
                    "ANC_DATE",
                    "END_DATE",
                    "INC_BIZ",
                    "NET_PFT",
                    "TTL_PFT"]
    },
    {
        "model": "FinancialIndicator",
        "table": "INFO_PARTY_FIN_INDDRV",
        "columns": ["PARTY_CODE",
                    "RPT_TYPE",
                    "RPT_TERM_TYPE",
                    "ANC_DATE_LAT",
                    "END_DATE",
                    "EPS_DIL_OP",
                    "ROA",
                    "GRO_PRO_MAR",
                    "GRO_MAR",
                    "ASS_RN",
                    "STM_IS",
                    "EXI_CUR",
                    "EXI_NON",
                    "ORPS",
                    "EBIT"]
    },
    {
        "model": "FinancialIndicator",
        "table": "INFO_PARTY_FIN_CASHFLOW",
        "columns": ["RPT_TYPE",
                    "RPT_TERM_TYPE",
                    "PARTY_CODE",
                    "ANC_DATE",
                    "END_DATE",
                    "PAY_OBT_FIOLTA",
                    "RECP_DISP_FIOLTA",
                    "ICF",
                    "OCF",
                    "ICR_CASH_EQLT",
                    "CASH_EQLT_EOP"]
    },
]

instrument_data = [
    {
        "model": "BondInfo",
        "table": "INFO_FI_IR_DETAILED",
        "columns": [
            "BOND_CODE",
            "IR_START_DATE",
            "BASE_IR",
            "IP_PAR_IR"
        ]
    },
    {
        "model": "BondInfo",
        "table": "INFO_FI_RIT_EMB_XCS",
        "columns": [
            "BOND_CODE",
            "RIT_TYPE",
            "XCS_THRT_DATE",
            "XCS_PRC",
            "XCS_IR_ADJ"
        ]
    },
    {
        "model": "BondInfo",
        "table": "INFO_FI_ADVANCE_REPAY",
        "columns": [
            "BOND_CODE",
            "ADV_PAY_MODE",
            "ADV_PAY_DATE",
            "ADV_PAY_RAT",
            "PAY_FIR_VALUE",
            "PAY_AFT_VALUE"
        ]
    },
    {
        "model": "BondInfo",
        "table": "INFO_FI_BONDISSUE",
        "columns": [
            "PAR_VALUE",
            "REF_FLD"
        ]
    },
    {
        "model": "BondInfo",
        "table": "INFO_FI_BASICINFO",
        "columns": ["BOND_CODE",
                    "CSNAME",
                    "BOND_SYMBOL",
                    "PAR_VALUE_ISS",
                    "EXCH_CODE",
                    "ACT_DUE_DATE",
                    "INT_RULE",
                    "VALUE_DATE",
                    "ISS_PRICE",
                    "PARTY_CODE",
                    "PAR_ANN_IR",
                    "CPN_TYPE",
                    "IR_TYPE",
                    "FLT_BM",
                    "FLT_IR_SPRD",
                    "PAY_FREQ",
                    "ANN_IR_FLOOR",
                    ]
    },

    {
        "model": "FundInfo",
        "table": "PROD_FUNDS",
        "columns": ["FUND_CODE",
                    "FUND_NAME"]
    },
    {
        "model": "StockIndexInfo",
        "table": "INFO_IDX_BASICINFO",
        "columns": ["IDX_CODE",
                    "CNAME",
                    "SYMBOL"]
    },

    {
        "model": "StockInfo",
        "table": "INFO_STK_BASICINFO",
        "columns": ["STK_CODE",
                    "SYMBOL",
                    "PARTY_CODE",
                    "LIST_STATUS",
                    "LIST_DATE",
                    "DELIST_DATE"]
    },
]

trade_data = [
    {
        "model": "BondTrade",
        "table": "INDIC_BASE_TRD_BOND",
        "columns": ["PRODUCT_CODE",
                    "ORDER_CODE",
                    "TRADE_DATE",
                    "SECU_CODE",
                    "TRADE_TYPE_CODE",
                    "TRAN_QTY",
                    "TRAN_AMT",
                    "SETTLE_AMT"]
    },
    {
        "model": "StockTrade",
        "table": "INDIC_BASE_TRD_STOCK",
        "columns": ["AMT",
                    "TRADE_FEE",
                    "SECU_CODE"]
    },
    {
        "model": "StockTrade",
        "table": "INDIC_BASE_POS_STOCK",
        "columns": ["PRODUCT_CODE",
                    "BUSI_DATE",
                    "SECU_CODE"]
    },
]

portfolio_data = [
    {
        "model": "StockIndexPortfolio",
        "table": "INFO_IDX_WT_STK",
        "columns": ["IDX_CODE",
                    "TRD_DATE",
                    "CPN_CODE",
                    "WT"]
    },
]

market_data = [
    {
        "model": "StockDailyQuote",
        "table": "INFO_PARTY_SHRSTRUC",
        "columns": ["PARTY_CODE",
                    "ANC_DATE",
                    "SHR_PS",
                    "CHG_DATE",
                    "SHR_TTL",
                    "SHR_CRC",
                    ]
    },
    {
        "model": "StockDailyQuote",
        "table": "INFO_STK_EODPRICE",
        "columns": ["PARTY_CODE",
                    "CLS_PRC_ADJ",
                    "PRE_CLS_PRC_ADJ",
                    "PCT_CHG",
                    "END_DATE",
                    "STK_CODE",
                    "AMT",
                    "CLS_PRC",
                    "PRE_CLS_PRC",
                    ]
    },
    {
        "model": "StockDailyQuote",
        "table": "INFO_STK_EODPRICE",
        "columns": ["CV",
                    "MV",
                    "PCT_CHG",
                    "DVD_YLD"
                    ]
    },
    {
        "model": "IndustryClassificationMktData",
        "table": "INFO_PARA_INFO",
        "columns": ["PARA_CODE",
                    "PARA_NAME"
                    ]
    },
    {
        "model": "IndustryClassificationMktData",
        "table": "INFO_PARTY_INDUSTRY",
        "columns": ["PARTY_CODE",
                    "START_DATE",
                    "INDU_SYS_CODE",
                    "INDU_CODE_1ST",
                    "INDU_NAME_1ST"
                    ]
    },
    {
        "model": "IndexRate",
        "table": "INFO_FI_CNBD_YIELD_CURV",
        "columns": ["TRD_DATE",
                    "CURV_CODE",
                    "STD_TERM",
                    "YIELD"
                    ]
    },
    {
        "model": "IndexRate",
        "table": "INFO_FI_BNK_DL_IR",
        "columns": ["IR_CODE",
                    "CHG_START_DATE",
                    "INTR_RAT"
                    ]
    },
    {
        "model": "BondValCSI",
        "table": "INFO_FI_VAL_CNBD",
        "columns": ["BOND_CODE",
                    "TRD_DATE",
                    "YIELD",
                    "MODIF_DUR",
                    "NET_PRC",
                    "FULL_PRC"
                    ]
    },
    {
        "model": "BondValCSI",
        "table": "INFO_FI_CASHFLOW",
        "columns": ["BOND_CODE",
                    "CASH_DATE",
                    "PAY_INT",
                    "PRCP_CASH"
                    ]
    },
    {
        "model": "BenchmarkDailyQuote",
        "table": "INFO_IDX_EODVALUE",
        "columns": ["TRD_DATE",
                    "CLS_PRC",
                    "PCT_CHG",
                    "IDX_CODE"
                    ]
    },
]

data = {
    "financial": financial_data,
    "instruments": instrument_data,
    "trade_info": trade_data,
    "portfolio": portfolio_data,
    "market": market_data
}


def get_column_comment(table_name, columns, database_name='plato_idx'):
    columns_value = ','.join(["'%s'" % i for i in columns])

    sql = f"""select
            COLUMN_NAME ,
            column_comment ,
            column_type 
--             column_key 
        from
            information_schema.columns
        where
            table_schema = '{database_name}'
            and table_name = '{table_name}'
            and COLUMN_NAME IN ({columns_value})
            """
    df = pd_read_sql(sql, session=database_name)
    df['table_name'] = table_name
    return df


def get_tb_info_file_path(file_name):
    schema_dir_path = os.path.join(settings.etl_save_path, 'schema')
    if not os.path.exists(schema_dir_path):
        os.makedirs(schema_dir_path)
    return os.path.join(schema_dir_path, file_name)


def get_etl_table_info():
    """获取表字段注释"""
    dfs = []
    for category, models in data.items():
        for item in models:
            table = item['table']
            model = item['model']
            columns = item['columns']
            df = get_column_comment(table_name=table, columns=columns)
            df['model_name'] = model
            df['category'] = category
            dfs.append(df)
    df = pd.concat(dfs)
    file_name = get_tb_info_file_path('table_info.xlsx')
    df.to_excel(file_name, index=False)
    return df


if __name__ == '__main__':
    print(get_etl_table_info())
