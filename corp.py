from enum import Enum


class Market(str, Enum):
    KOSPI = 'KOSPI'
    KOSDAQ = 'KOSDAQ'


class Corp:
    def __init__(self, name:str, stock:str, market:Market):
        self.name = name
        self.stock = stock
        self.market = market
        self.sales = None
        self.net_income = None
        self.profit = None
        self.cash_flow = None
        #self.book_value = None
        self.assets = None
        self.liabilities = None
        self.price = None
        self.shares = None
        self.capex = None

        self.market_cap = None

        self.pbr = None
        self.per = None
        self.psr = None
        self.fcf = None
        self.pfcr = None

        self.ipbr = None
        self.iper= None
        self.ipsr = None
        self.ipfcr = None


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


    def cal_indicators(self):
        self.pbr = self.__pbr()
        self.per = self.__per()
        self.psr = self.__psr()
        self.pfcr = self.__pfcr()

        self.ipbr = 1. / self.ipbr
        self.iper = 1. / self.iper
        self.ipsr = 1. / self.ipsr
        self.ipfcr = 1./ self.ipfcr

        self.market_cap = self.price * self.shares