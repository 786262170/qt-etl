import os
import time

import pandas as pd

from qt_etl.entity.instruments import BondInfo, FundInfo, StockIndexInfo, StockInfo, BenchMarkInfo
from qt_etl.entity.market_data import BenchmarkDailyQuote, BondValCSI, \
    IndustryClassificationMktData, \
    StockDailyQuote, IndexRate
from qt_etl.entity.portfolio import CombPosition, StockIndexPortfolio
from qt_etl.entity.trade_info import BondTradeInfo, StockTradeInfo
from qt_quant.scripts import schema_dir_path
from qt_etl.scripts import schema_dir_path

portfolio_model_list = [CombPosition, StockIndexPortfolio]
trade_model_list = [BondTradeInfo, StockTradeInfo]
instrument_model_list = [BondInfo, FundInfo, BenchMarkInfo, StockIndexInfo, StockInfo]
market_model_list = [BenchmarkDailyQuote, BondValCSI, IndustryClassificationMktData,
                     StockDailyQuote, IndexRate]

total_model = portfolio_model_list + trade_model_list + instrument_model_list + market_model_list

tab_space = '    '


def create_schema(etl_rename_dict, xlsx_path, model_name):
    """
    生成schema
    """
    df = pd.read_excel(xlsx_path)

    columns = []
    metadata_columns = []
    data = etl_rename_dict.split('\n')
    for i in data:
        if ':' not in i:
            continue
        i = i.strip()
        item = i.strip(',')
        key, val = item.split(":")
        column_key = key.strip("'")
        comment_data = df[(df['COLUMN_NAME'] == column_key) & (df['model_name'] == model_name)]

        comment = ''
        column_type = ''
        if not len(comment_data):
            print(f'column_name:{column_key}, model_name:{model_name} not fund')
            continue
        comment_data = comment_data.iloc[0]
        comment = comment_data['COLUMN_COMMENT']
        column_type = comment_data['COLUMN_TYPE']

        schema_column_type = "pa.string()"
        if column_type.startswith('decimal') or column_type.startswith('double'):
            schema_column_type = "pa.float64()"
        column = "pa.field(%s, %s, metadata={b'table_field': b%s})," % (val, schema_column_type, key)
        metadata_column = f"{val}: '{comment}',"
        if len(columns):
            column = f"{tab_space * 2}{column}"
        columns.append(column)
        if len(metadata_columns):
            metadata_column = f"{tab_space * 3}{metadata_column}"
        metadata_columns.append(metadata_column)

    column_res = '\n'.join(columns)
    metadata_res = '\n'.join(metadata_columns)
    schema = f"""schema = pa.schema([
        {column_res}
    ],
        metadata={{
            {metadata_res}
        }}
    )
    """
    print(schema)
    return schema


def create_schema_by_columns(etl_rename_dict, xlsx_path, model_name, etl_df):
    """
    生成schema
    """
    df = pd.read_excel(xlsx_path)
    filed_dimension_dict = {}
    real_name_field_dict = {}
    data = etl_rename_dict.split('\n')
    for i in data:
        if ':' not in i:
            continue
        i = i.strip()
        item = i.strip(',')
        print(i)
        key, val = item.split(":")
        key = key.strip("'")
        val = val.strip()
        val = val.strip("'")
        filed_dimension_dict[key] = val
        real_name_field_dict[eval(val)] = key
    print(real_name_field_dict)
    exit()
    columns = []
    metadata_columns = []
    for column in etl_df.columns:
        column = column
        field_name = real_name_field_dict.get(column)

        dimension_name = filed_dimension_dict.get(field_name)
        comment_data = df[(df['COLUMN_NAME'] == field_name) & (df['model_name'] == model_name)]
        comment = ''
        column_type = ''
        if not len(comment_data):
            print(f'field_name:{field_name}, model_name:{model_name} not fund')
            continue
        comment_data = comment_data.iloc[0]
        comment = comment_data['COLUMN_COMMENT']
        column_type = comment_data['COLUMN_TYPE']

        schema_column_type = "pa.string()"
        df_type = etl_df[column].dtypes.name
        if df_type.startswith('float'):
            schema_column_type = "pa.float64()"

        column = "pa.field(%s, %s, metadata={b'table_field': b'%s'})," % (
            dimension_name, schema_column_type, field_name)
        metadata_column = f"{dimension_name}: '{comment}',"
        if len(columns):
            column = f"{tab_space * 2}{column}"
        columns.append(column)
        if len(metadata_columns):
            metadata_column = f"{tab_space * 3}{metadata_column}"
        metadata_columns.append(metadata_column)

    column_res = '\n'.join(columns)
    metadata_res = '\n'.join(metadata_columns)
    schema = f"""schema = pa.schema([
        {column_res}
    ],
        metadata={{
            {metadata_res}
        }}
    )
    """
    print(schema)
    return schema


def export_schema():
    dfs = []
    for model in total_model:
        df = model.get_schema_df()
        dfs.append(df)
    schema_df = pd.concat(dfs)
    file_name = os.path.join(schema_dir_path, 'schema_df.xlsx')
    schema_df.to_excel(file_name, index=False)
    return schema_df


def check_etl():
    data = []
    for model in total_model:
        start_time = time.time()
        df = model.get_data()
        partitioned_by_date = model.partitioned_by_date
        partitioned_cols = model.partitioned_cols
        used_time = time.time() - start_time
        item = {
            "load_time": used_time,
            "length": len(df),
            "model": model.__name__,
            "partitioned_by_date": '' if isinstance(partitioned_by_date, bool) else partitioned_by_date,
            "partitioned_cols": partitioned_cols,
        }
        data.append(item)
    df = pd.DataFrame(data)

    df.to_excel(os.path.join(schema_dir_path, 'load_time.xlsx'))
    return df


if __name__ == '__main__':
    table_info_file = os.path.join(schema_dir_path, 'table_info.xlsx')
    etl_rename_dict = """ etl_rename_dict = {
        'BOND_CODE': Dimension.INSTRUMENT_CODE,
        'CSNAME': Dimension.INSTRUMENT_NAME,
        'BOND_SYMBOL': Dimension.SYMBOL,
        'PAR_VALUE_ISS': Dimension.PAR_VALUE_ISS,
        'PAR_VALUE': Dimension.PAR_VALUE,
        'REF_FLD': Dimension.REFERENCE_RATE_ISSUE,
        'EXCH_CODE': Dimension.TRADING_MARKET,
        'ACT_DUE_DATE': Dimension.MATURITY_DATE,
        'INT_RULE': Dimension.DAY_COUNT,
        "VALUE_DATE": Dimension.START_DATE,
        "ISS_PRICE": Dimension.ISSUE_PRICE,
        "PARTY_CODE": Dimension.ISSUER_CODE,
        "PAR_ANN_IR": Dimension.PAR_RATE_ISSUE,
        "CPN_TYPE": Dimension.COUPON_TYPE,
        "IR_TYPE": Dimension.INTEREST_RATE_TYPE,
        "FLT_BM": Dimension.BENCHMARK,
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
        'XCS_IR_ADJ': Dimension.ADJUST_ISSUE_RATE,
        'IR_START_DATE': Dimension.TRADE_DATE,
        'BASE_IR': Dimension.BASE_INTEREST_RATE,
        'IP_PAR_IR': Dimension.INTEREST_PERIOD_INTEREST_RATE,
        'ANN_IR_FLOOR': Dimension.ANN_IR_FLOOR  # 债券保底利率
    }
"""

    model_name = 'BondInfo'
    from qt_etl.entity.instruments import BondInfo

    etl_df = BondInfo.get_data().head()
    print(etl_df.columns)
    print(len(etl_df.columns))

    create_schema_by_columns(etl_rename_dict, table_info_file, model_name, etl_df)

    # export_schema()
    # print(check_etl())
