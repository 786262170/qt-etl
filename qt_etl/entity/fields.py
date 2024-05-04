# vim set fileencoding=utf-8
"""字段定义"""
import enum


class Measure:
    HIGH = 'high'
    OPEN = 'open'
    LOW = 'low'
    CLOSE = 'close'
    CLOSE_ADJUSTED = 'close_adjusted'
    PREV_CLOSE_ADJUSTED = 'prev_close_adjusted'
    PREV_CLOSE = 'prev_close'
    PREVIOUS_CLOSE = 'previous_close'
    TRADING_FEE = 'trading_fee'
    RETURN_RATE = 'return_rate'
    INVEST_RATE = 'invest_rate'
    # 涨跌幅(%)
    RETURN_PERCENTAGE = 'return_percentage'
    RETURN_PERCENTAGE_ADJUSTED = 'return_percentage_adjusted'
    RETURN_PERCENTAGE_ADJUSTED_LOG = 'return_percentage_adjusted_log'
    WEIGHT = 'weight'
    YIELD = "ytm"
    FAIR_VALUE = "fair_price"
    DIRTY_PRICE = "dirty_price"
    CLEAN_PRICE = "clean_price"
    DURATION = "duration"
    INTEREST = "interest"
    PRINCIPAL = "principal"
    IS_OPT = "is_option"
    # TRADE_YTM = "trade_ytm"
    TRADE_NOMINAL = "notional"
    VOLUME = 'volume'
    BUY_AMOUNT = 'buy_amount'
    SELL_AMOUNT = 'sell_amount'
    TRADE_AMOUNT = "trade_amount"
    SETTLE_AMOUNT = "settlement_amount"
    INTEREST_INCOME = "interest_income"
    RATE = 'rate'
    VOLATILITY = 'volatility'
    MARKET_VALUE = "market_value"
    PREV_MARKET_VALUE = "prev_market_value"
    QUANTITY = "quantity"
    PREV_QUANTITY = "prev_quantity"
    UNIT_COST = "unit_cost"
    COST = "cost"
    PREV_COST = 'prev_cost'
    # 摊余成本
    RMN_COST = 'rmn_cost'
    # 影子价值净值
    SHADOW_MARKET_VALUE = 'shadow_market_value'


    VALUATION_PRICE = "valuation_price"
    VALUATION_FULL_PRICE = "valuation_full_price"
    VALUATION_NET_PRICE = "valuation_net_price"

    DEPOSIT_LOAN_INTEREST_RATE = 'deposit_loan_interest_rate'

    # 周转率/换手率
    TURNOVER = 'turnover'
    NEGOTIABLE_MARKET_CAPITALIZATION = 'negotiable_market_capitalization'
    MARKET_CAPITALIZATION = 'market_capitalization'  # 市值
    DIVIDEND_YIELD = 'dividend_yield'
    PREFERRED_STOCK = 'preferred_stock'
    SHARES_OUTSTANDING = 'shares_outstanding'
    CIRCULATING_SHARES = 'circulating_shares'

    IS_RCM = "is_rcm"
    VAL_TYPE = "val_type"
    RCM_DIR = "rcm_dir"
    EXCH_RATE = "exch_rate"  # 汇率
    ACR_INT = 'acr_int' # 应计利息
    CASH_AMT_AFT = 'cash_amt_aft' # 偿还后期末金额(元)

    RISK_ORDER = 'risk_order'
    RISK_AMOUNT = 'risk_amount'
    # 组合净资金流入金额
    NET_CPTL_INFLOW_AMT = 'net_cptl_inflow_amt'
    LEVERAGE_RATE = 'leverage_rate'
    CASH_RATIO = 'cash_ratio'
    INST_POSI_RIN = 'inst_posi_rin'
    REALIZABLE_MARKET_VALUE ='realizable_market_value'


class Currency(enum.Enum):
    CNY = 'CNY'  # 人民币
    CNH = 'CNH'
    EUR = 'EUR'  # 欧元
    USD = 'USD'  # 美元
    HKD = 'HKD'  # 港币
    YEN = 'YEN'  # 日元

    def __str__(self):
        return self.value.upper()


class Dimension:
    INSTRUMENT_TYPE = 'instrument_type'
    INSTRUMENT_CODE = 'instrument_code'
    INSTRUMENT_NAME = 'instrument_name'
    BOND_TYPE = 'bond_type'
    IRS_TYPE = 'irs_type'
    TRADING_MARKET = 'market'
    SYMBOL = 'symbol'
    BOND_TERM_DAY = 'bond_term_day'
    BOND_TERM_YEAR = 'bond_term_year'
    PAR_VALUE_ISS = 'par_value_iss'
    PAR_VALUE = 'par_value'
    REFERENCE_RATE_ISSUE = 'reference_rate_issue'
    PAR_VALUE_LIST = 'par_value_list'
    ESTIMATED_MATURITY_DATE = 'estimated_maturity_date'
    ISS_START_DATE = 'iss_start_date'
    MATURITY_DATE = 'maturity_date'
    LIST_STATUS = 'list_status'
    LIST_DATE = 'list_date'
    DELIST_DATE = 'delist_date'
    EXCHANGE_CODE = 'exchange_code'
    DAY_COUNT = 'day_count'
    ISSUER_CODE = 'issuer_code'
    ISSUE_NAME = 'issuer_name'
    START_DATE = 'start_date'
    ISSUE_PRICE = 'issue_price'
    PAR_RATE_ISSUE = 'par_rate_issue'
    COUPON_TYPE = 'coupon_type'
    INTEREST_RATE_TYPE = 'interest_rate_type'
    # BENCHMARK = 'benchmark'
    BENCHMARK_CODE = 'benchmark_code'
    SPREAD = 'spread'
    PAY_FREQUENCY = 'pay_frequency'
    COMPOUNDING = 'compounding'
    First_Interest_Date = 'first_interest_date'
    INTEREST_ON_FEB_29 = 'interest_on_feb_29'
    BOOK_ID = "book_id"
    BOOK_NAME = "name"
    CHILDREN_BOOK_ID = "children_book_id"
    TRADE_ID = "trade_id"
    TRADE_TYPE = 'trade_type'
    TRADE_DATE = "trade_date"
    # CHANGE_DATE = "change_date"
    INTEREST_START_DATE = "interest_start_date"
    INTEREST_END_DATE = "interest_end_date"

    CURRENCY = "currency"
    COUNTERPARTY = 'counterparty'
    TIME_SERIES_TYPE = 'time_series_type'
    VOLATILITY_TYPE = 'volatility_type'
    MONEYNESS = 'moneyness'

    # 截止日期
    DUE_DATE = 'due_date'
    HELD_TO_MATURITY = "held_to_maturity"
    # 是否穿透
    DOWN_FLAG = 'down_flag'

    MKT_OBJ_NAME = 'mkt_obj_name'
    QUALIFIER = 'qualifier'
    INDEX = 'index'
    INDEX_NAME = 'index_name'
    TENOR = 'tenor'
    FORWARD_TENOR = 'forward_tenor'
    TENOR2 = 'tenor2'
    RATE = 'rate'
    RISK_TYPE = 'risk_type'
    MKT_INFO = 'mkt_info'
    INDUSTRY_CODE = 'industry_code'
    INDUSTRY_SYS_CODE = 'industry_sys_code'
    INDUSTRY_NAME = 'industry_name'
    INSIDE_BENCHMARK = 'inside_benchmark'

    SECTOR_CODE = 'sector_code'
    SECTOR_NAME = 'sector_name'

    SECU_TYPE_CODE = 'secu_type_code'
    CLS_CODE_1ST = 'CLS_CODE_1ST'
    CLS_CODE_2ND = 'CLS_CODE_2ND'
    CLS_CODE_3RD = 'CLS_CODE_3RD'
    CLS_CODE_4TH = 'CLS_CODE_4TH'
    SEC_TYPE = 'sec_type'

    REPORT_TYPE = 'report_type'
    REPORTING_PERIOD_TYPE = 'reporting_period_type'

    ADVANCE_REPAY_MODE = "advance_repay_mode"
    ADVANCE_REPAY_RATIO = "advance_repay_ratio"
    PAY_FIR_VALUE = 'pay_fir_value'
    PAY_AFT_VALUE = 'pay_aft_value'
    ADJUST_ISSUE_RATE = "adjust_issue_rate"
    BASE_INTEREST_RATE = 'base_interest_rate'
    INTEREST_PERIOD_INTEREST_RATE = 'interest_period_interest_rate'
    RIT_TYPE = 'rit_type'
    HIGH_INTEREST_RATE_ADJ = "high_interest_rate_adj"
    LOW_INTEREST_RATE_ADJ = "low_interest_rate_adj"
    STRIKE_PRICE = "strike_price"
    ANN_IR_FLOOR = "ann_ir_floor"
    CALC_WAY = "calc_way"
    FRN_ADJUST_RT = "frn_adjust_rate"
    NOTIONAL_REDUCE_MAP = "notional_reduce_map"
    COUPON_CHANGE_MAP = "coupon_change_map"
    CALL_OPTION_MAP = "call_option_map"
    PUT_OPTION_MAP = "put_option_map"
    FLOAT_RATE_RECORDS = "float_rate_records"
    BENCHMARK_DATES = "benchmark_dates"
    BENCHMARK_VALUES = "benchmark_values"
    ADJUST_NUM_FOR_LASTEST = "adjust_num_for_lastest"
    FLOAT_TYPE = "float_type"
    COUPON_ACCURACY = "coupon_accuracy"
    IS_CREATE_NEW = "is_create_new"
    SETTLE_DATE = 'settle_date'
    FORWARD_MATRUITY_CURVE = 'forward_maturity_curve'
    FORWARDSPOTCURVE = 'forward_spot_curve'

    CURVE_CODE = 'curve_code'
    CURVE_TYPE = 'curve_type'
    CURVE_TYPE_NAME = 'curve_type_name'

    RATING = 'rating'
    RATING_TYPE = 'rating_type'
    RATING_ORGANIZATION = 'rating_organization'

    REMN_TERM = "remn_term"
    STD_TERM = "std_term"

    SCENE_ID = 'scene_id'
    FACTOR_TYPE = 'factor_type'
    FACTOR_NAME = 'factor_name'
    MODIFICATION_TYPE = 'modification_type'
    MODIFICATION_VALUE = 'modification_value'

    FX_CODE = "fx_code"  # 货币对代码
    # 资产分类
    CLS_CODE = 'cls_code'
    CLS_NAME = 'cls_name'
    CLS_TYPE = 'cls_type'
    TYPE_CODE = 'type_code'
    TYPE_NAME = 'type_name'
    P_TYPE_CODE = 'p_type_code'



class Fundamental:
    TOTAL_NON_CURRENT_LIABILITIES = 'total_non_current_liabilities'
    TOTAL_LIABILITIES = 'total_liabilities'
    TOTAL_ASSETS = 'total_assets'
    MONETARY_FUND = 'monetary_fund'
    HELD_FOR_TRADING_FINANCIAL_ASSETS = 'held_for_trading_financial_assets'
    NOTES_RECEIVABLE = 'notes_receivable'
    TOTAL_INVESTORS_EQUITY = 'total_investors_equity'
    MINORITY_INTEREST = 'minority_interest'

    """PARTY_FIN_INCOME 利润表"""
    # 营业收入    INC_BIZ,
    PERATING_INCOME = 'perating_income'

    # 报表类型    RPT_TYPE,
    # 主体内部编码    PARTY_CODE,
    # 截止日期    END_DATE,
    # 净利润    NET_PFT,
    NET_RETURN = 'net_return'
    # 息税前利润    EBIT,
    EBIT = 'ebit'
    # 信息发布日期    ANC_DATE,
    # 利润总额    TTL_PFT
    TOTAL_PROFIT = 'total_profit'

    """PARTY_FIN_CASHFLOW现金流量表"""

    # 报表类型    RPT_TYPE,
    # 报告期类型    RPT_TERM_TYPE,
    # 主体内部编码    PARTY_CODE,
    # 信息发布日期    ANC_DATE,
    # 购建固定资产、无形资产和其他长期资产支付的现金    PAY_OBT_FIOLTA,
    CASH_PAID_FOR_LONG_TERM_ASSETS = 'cash_paid_for_long_term_assets'
    # 处置固定资产、无形资产和其他长期资产所收回的现金净额 RECP_DISP_FIOLTA
    NET_CASH_RECOVERED = 'net_cash_recovered'
    # 截止日期    END_DATE,
    # 投资活动产生的现金流量净额    ICF,
    INVESTMENT_ACTIVITIES_CASH_FLOW = 'investment_activities_cash_flow'
    # 经营活动产生的现金流量净额    OCF,
    OCF = 'ocf'
    # 现金及现金等价物净增加额    ICR_CASH_EQLT,
    NET_INCREASE_IN_EQUIVALENTS = 'net_increase_in_equivalents'
    # 期末现金及现金等价物余额    CASH_EQLT_EOP
    EQUIVALENTS_BALANCE = 'equivalents_balance'
    # 公司内码    PARTY_CODE,
    # 报表类型    RPT_TYPE,
    # 末次公告日期    ANC_DATE_LAT,
    # 摊薄每股收益(营业利润)(元)    EPS_DIL_OP,
    OPERATING_PROFIT = 'operating_profit'
    # 总资产净利率_平均(含少数股东损益的净利润)(%)    ROA,
    RETURN_ON_TOTAL_ASSETS_FLOAT = 'return_on_total_assets_float'
    # 销售毛利率(%))    GRO_PRO_MAR,
    GROSS_PROFIT_MARGIN_FLOAT = 'gross_profit_margin_float'
    # 营业总收入(元)    GRO_MAR,
    TOTAL_OPERATING_INCOME = 'total_operating_income'
    # 总资产周转率(次)    ASS_RN,
    TOTAL_ASSETS_TURNOVER = 'total_assets_turnover'
    # 当期计提折旧与摊销(元)    STM_IS,
    AMORTIZATION_AND_ACCRUAL = 'amortization_and_accrual'
    # 截止日期    END_DATE,
    # 无息流动负债(元)    EXI_CUR,
    INTEREST_FREE_CURRENT_LIABILITIES = 'interest_free_current_liabilities'
    # 无息非流动负债(元)    EXI_NON,
    INTEREST_FREE_NON_CURRENT_LIABILITIES = 'interest_free_non_current_liabilities'
    # 每股营业收入(元)     ORPS
    EARNINGS_PER_SHARE = 'earnings_per_share'
    RETURN_ON_ASSETS = 'returnon_assets'


class Factor:
    LN_NMV = 'ln_nmv'
    MID_CAP = 'mid_cap'
    BETA = 'beta'
    HISTORICAL_SIGMA = 'historical_sigma'
    DAILY_STD = 'daily_std'
    CUMULATIVE_RETURN_RANGE = 'cumulative_return_range'
    MONTHLY_TURNOVER = 'monthly_turnover'
    QUARTERLY_TURNOVER = 'quarterly_turnover'
    ANNUAL_TURNOVER = 'annual_turnover'
    ANNUALIZED_TRADED_VALUE_RATIO = 'annualized_traded_value_ratio'
    SHORT_TERM_REVERSAL = 'short_term_reversal'
    SEASONALITY = 'seasonality'
    INDUSTRY_MOMENTUM = 'industry_momentum'
    RELATIVE_STRENGTH = 'relative_strength'
    HISTORICAL_ALPHA = 'historical_alpha'
    MARKET_LEVERAGE = 'market_leverage'
    BOOK_LEVERAGE = 'book_leverage'
    DEBT_TO_ASSET_RATIO = 'debt_to_asset_ratio'
    VARIATION_IN_SALES = 'variation_in_sales'
    VARIATION_IN_EARNINGS = 'variation_in_earnings'
    VARIATION_IN_CASH_FLOWS = 'variation_in_cashflow'
    PREDICTED_EARNING_TO_PRICE_RATIO = 'predicted_earning_to_price_ratio'
    ACCRUALS_BALANCE_SHEET = 'accruals_balance_sheet'
    ACCRUALS_CASH_FLOW = 'accruals_cashflow'
    ASSET_TURNOVER = 'asset_turnover'
    GROSS_PROFITABILITY = 'gross_profit_ability'
    GROSS_PROFIT_MARGIN = 'gross_profit_margin'
    RETURN_ON_ASSETS = 'return_on_assets'
    TOTAL_ASSETS_GROWTH_RATE = 'total_assets_growth_rate'
    ISSUANCE_GROWTH_RATE = 'issuance_growth_rate'
    CAPITAL_EXPENDITURE_GROWTH_RATE = 'capital_expenditure_growth_rate'
    BOOK_TO_PRICE = 'book_to_price'
    TRAILING_EARNINGS_TO_PRICE_RATIO = 'trailing_earnings_to_price_ratio'
    ANALYST_PREDICTED_EARNINGS_TO_PRICE = 'analyst_predicted_earnings_to_price'
    CASH_EARNINGS_TO_PRICE = 'cash_earnings_to_price'
    ENTERPRISE_MULTIPLE = 'enterprise_multiple'
    LONG_TERM_RELATIVE_STRENGTH = 'long_term_relative_strength'
    LONG_TERM_HISTORICAL_ALPHA = 'long_term_relative_alpha'
    PREDICTED_GROWTH_THREE_YEARS = 'predicted_growth_three_years'
    HISTORICAL_EARNINGS_PER_SHARE_GROWTH_RATE = 'historical_earnings_per_share_growth_rate'
    HISTORICAL_SALES_PER_SHARE_GROWTH_RATE = 'historical_sales_per_share_growth_rate'
    REVISION_RATIO = 'revision_ratio'
    CHANGE_IN_ANALYST_PREDICTED_EARNINGS_TO_PRICE = 'change_in_analyst_predicted_earnings_to_price'
    CHANGE_IN_ANALYST_PREDICTED_EARNINGS_PER_SHARE = 'change_in_analyst_predicted_earnings_per_share'
    DIVIDEND_TO_PRICE_RATIO = 'dividend_to_price_ratio'
    ANALYST_PREDICTED_DIVIDEND_TO_PRICE_RATIO = 'analyst_predicted_dividend_to_price_ratio'
    RESIDUAL_VOLATILITY = "residual_volatility"
    LIQUIDITY = "liquidity"
    LEVERAGE = "leverage"
    EARNINGS_VARIABILITY = "earnings_variability"
    PROFITABILITY = "profitability"
    EARNINGS_QUALITY = "earnings_quality"
    EARNING_YIELD = "earning_yield"
    INVESTMENT_QUALITY = "investment_quality"
    GROWTH = "growth"
    LONG_TERM_REVERSAL = "long_term_reversal"
    ANALYST_SENTIMENT = "analyst_sentiment"
    QUALITY = "quality"
    VOLATILITY = "volatility"
    MOMENTUM = "momentum"
    VALUE = "value"
    SENTIMENT = "sentiment"
    SIZE = 'size'
    NON_LINEAR_SIZE = 'non_linear_size'
    COMOVEMENT = 'comovement'


class InstrumentType(enum.Enum):
    FUND = '基金'
    BENCH_MARK = '基准'
    BOND = '债券'
    STOCK = '股票'
    ABS = "资产支持证券"
    FUTURE = "期货"
    REPO = "回购"
    NONSTD = "非标债权"
    AMP = "资产管理计划"
    DEPOSIT = "存款"
    CASH_OTHER = "现金"
    CASH = "现金"
    INDUSTRY = '行业'
    STOCK_INDEX = '股票指数'
    MIX_ASSET = '混合资产'
    IRS = '利率互换'


class IndustryType:
    """行业分类"""
    BANK = "bank"
    COMPUTER = "computer"
    ENVIRONMENT_PROTECTION = "environment_protection"
    TRADE_RETAIL = "trade_retail"
    POWER_EQUIPMENT = "power_equipment"
    ARCHITECTURAL_DECORATION = "architectural_decoration"
    BUILDING_MATERIAL = "building_material"
    AFHF = "afhf"
    ELECTRONICS = "electronics"
    TRANSPORTATION = "transportation"
    AUTOMOBILE = "automobile"
    TEXTILE_CLOTHING = "textile_clothing"
    MEDICAL_BIOLOGY = "medical_biology"
    REAL_ESTATE = "real_estate"
    COMMUNICATION = "communication"
    PUBLIC_UTILITY = "public_utility"
    COMPREHENSIVE = "comprehensive"
    MECHANICAL_EQUIPMENT = "mechanical_equipment"
    PETROLEUM_PETROCHEMICAL = "petroleum_petrochemical"
    NON_FERROUS_METALS = "non_ferrous_metals"
    MEDIA = "media"
    HOUSEHOLD_ELECTRIC_APPLIANCES = "household_electric_appliances"
    BASIC_CHEMICAL = "basic_chemical"
    NON_BANK_FINANCE = "non_bank_finance"
    SOCIAL_SERVICES = "social_services"
    LIGHT_INDUSTRY_MANUFACTURING = "light_industry_manufacturing"
    NATIONAL_DEFENSE = "national_defense"
    BEAUTY_CARE = "beauty_care"
    COAL = "coal"
    FOOD_BEVERAGE = "food_beverage"
    STEEL = "steel"


class TimeSeriesType(enum.Enum):
    ABSOLUTE_YIELD_CURVE_CHANGE = 'absolute_yield_curve_change'
    RELATIVE_YIELD_CURVE_CHANGE = 'relative_yield_curve_change'
    PRICE = 'price'
    ADJUSTED_PRICE = 'adjusted_price'
    RETURN = 'return'
    ADJUSTED_RETURN = 'adjusted_return'
    LOG_RETURN = 'log_return'
    ADJUSTED_LOG_RETURN = 'adjusted_log_return'
    YIELD_CURVE = 'yield_curve'
    DEPOSIT = 'deposit'
    IRS_CURVE = 'irs_curve'


