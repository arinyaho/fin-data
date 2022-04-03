from __future__ import annotations
from enum import Enum


class Market(str, Enum):
    KOSPI = 'KOSPI'
    KOSDAQ = 'KOSDAQ'


class Corp:
    def __init__(self, name:str, stock:str, market:Market, year:int, quarter:int):
        self.name = name
        self.stock = stock
        self.market = market
        self.year = year
        self.quarter = quarter

        self.sales = None
        self.sales_cost = None
        self.net_income = None
        self.profit = None
        self.cash_flow = None
        self.assets = None
        self.equity = None
        self.liabilities = None
        self.price = None
        self.shares = None
        self.equity_issue = 0
        self.capex_intangible = 0
        self.capex_property = 0

        self.capex = None
        self.market_cap = None
        self.sales_profit = None
        self.book_value = None

        self.pbr = None
        self.per = None
        self.psr = None
        self.pcr = None
        self.fcf = None
        self.pfcr = None
        self.roa = None
        self.roe = None
        self.gpa = None

        self.ipbr = None
        self.iper= None
        self.ipsr = None
        self.ipfcr = None
        self.ipcr = None

        self.qoq_net_income = None
        self.qoq_profit = None
        self.qoq_book_value = None
        self.qoq_assets = None

        self.yoy_net_income = None
        self.yoy_profit = None

        self.profit_growth_qoq = None
        self.profit_growth_yoy = None
        self.net_income_growth_qoq = None
        self.net_income_growth_yoy = None
        self.book_value_growth_qoq = None
        self.assets_growth_qoq = None
        
        # Orders
        self.ord_ipbr = None
        self.ord_iper= None
        self.ord_ipsr = None
        self.ord_ipfcr = None
        self.ord_ipcr = None
        self.ord_profit_growth_qoq = None
        self.ord_profit_growth_yoy = None
        self.ord_net_income_growth_qoq = None
        self.ord_net_income_growth_yoy = None
        self.ord_book_value_growth_qoq = None
        self.ord_assets_growth_qoq = None
        
        # Scores
        self.fscore_k = None