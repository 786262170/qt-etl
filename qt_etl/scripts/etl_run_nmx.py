# coding=utf8

from qt_etl.entity.market_data import FiYieldCurve, VolatilitySurface


def main():
    """Numerix应用Etl初始化脚本"""
    FiYieldCurve.run_etl()  # 外汇交易中心收益率曲线
    VolatilitySurface.run_etl(is_init=True)


if __name__ == '__main__':
    main()
