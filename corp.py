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
        self.capex = None
        self.equity_issue = 0



        self.market_cap = None
        self.sales_profit = None
        self.book_value = None

        self.pbr = None
        self.per = None
        self.psr = None
        self.fcf = None
        self.pfcr = None
        self.roa = None
        self.roe = None
        self.gpa = None

        self.ipbr = None
        self.iper= None
        self.ipsr = None
        self.ipfcr = None

        self.profit_growth_qoq = None
        self.profit_growth_yoy = None
        self.net_income_growth_qoq = None
        self.net_income_growth_yoy = None
        self.bool_value_grwoth_qoq = None
        self.asset_growth_qoq = None
        self.fscore_k = None


    def __per(self):
        try:
            return self.price * self.shares / self.net_income
        except ZeroDivisionError:
            return float('inf')

    
    def __pbr(self):
        try:
            return self.price * self.shares / (self.assets - self.liabilities)
        except ZeroDivisionError:
            return float('inf')
    

    def __psr(self):
        try:
            return self.price * self.shares / self.sales
        except ZeroDivisionError:
            return float('inf')


    def __fcf(self):
        return self.cash_flow - self.capex

    
    def __pfcr(self):
        try:
            return self.price * self.shares / self.fcf()
        except ZeroDivisionError:
            return float('inf')
    
   
    def has_full_data(self):
        return all(map(lambda v: v is not None, self.__dict__.values()))


    def cal_indicators(self, qoq: Corp, yoy: Corp):
        self.pbr = self.__pbr()
        self.per = self.__per()
        self.psr = self.__psr()
        self.pfcr = self.__pfcr()

        self.roa = self.net_income / (self.equity + self.liabilities)
        self.roe = self.net_income / self.equity

        self.ipbr = 1. / self.ipbr
        self.iper = 1. / self.iper
        self.ipsr = 1. / self.ipsr
        self.ipfcr = 1./ self.ipfcr        

        self.market_cap = self.price * self.shares
        self.sales_profit = self.sales - self.sales_cost
        self.book_value = self.assets - self.liabilities
        self.gpa = self.sales_profit / self.book_value

        self.profit_growth_qoq = self.profit / qoq.profit
        self.profit_growth_yoy = self.profit / yoy.profit
        self.net_income_growth_qoq = self.net_income / qoq.net_income
        self.net_income_growth_yoy = self.net_income / yoy.net_income
        self.bool_value_grwoth_qoq = self.book_value / yoy.book_value
        self.asset_growth_qoq = self.assets / qoq.assets
        
        self.fscore_k = 0
        if self.equity_issue > 0:
            self.fscore_k += 1
        if self.net_income > 0:
            self.fscore_k += 1
        if self.cash_flow > 0:
            self.fscore_k += 1
