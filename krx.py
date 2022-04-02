from typing import Dict, List
from xxlimited import Str
from corp import Market, Corp
import re, json, os, traceback


# http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020101

# 매출액
_dart_code_sales1 = 'ifrs_Revenue'
_dart_code_sales2 = 'ifrs-full_Revenue'

# 매출원가
_dart_code_sales_cost1 = 'ifrs_CostOfSales'
_dart_code_sales_cost2 = 'ifrs-full_CostOfSales'

# 당기순이익
_dart_code_net_income1 = 'ifrs_ProfitLoss'
_dart_code_net_income2 = 'ifrs-full_ProfitLoss'


# 영업이익
_dart_code_profit1 = 'dart_OperatingIncomeLoss'


# 자산총계
_dart_code_assets1 = 'ifrs_Assets'
_dart_code_assets2 = 'ifrs-full_Assets'

# 자본총계
_dart_code_equity1 = 'ifrs_Assets'
_dart_code_equity2 = 'ifrs-full_Assets'

# 유상증자
_dart_code_euqity_issue1 = 'ifrs-full_IssueOfEquity'
_dart_code_euqity_issue2 = 'ifrs_IssueOfEquity'

# 현금흐름
_dart_code_cash_flow1 = 'ifrs_CashFlowsFromUsedInOperatingActivities'
_dart_code_cash_flow2 = 'ifrs-full_CashFlowsFromUsedInOperatingActivities'

# 부채총계
_dart_code_liabilities1 = 'ifrs_Liabilities'
_dart_code_liabilities2 = 'ifrs-full_Liabilities'

# 무형자산의 취득
_dart_code_capex1 = 'ifrs_PurchaseOfIntangibleAssetsClassifiedAsInvestingActivities'
_dart_code_capex2 = 'ifrs-full_PurchaseOfIntangibleAssetsClassifiedAsInvestingActivities'

# 유형자산의 취득
_dart_code_capex3 = 'ifrs_PurchaseOfPropertyPlantAndEquipmentClassifiedAsInvestingActivities'
_dart_code_capex4 = 'ifrs-full_PurchaseOfPropertyPlantAndEquipmentClassifiedAsInvestingActivities'

_dart_kospi_title = '유가증권시장상장법인'
_dart_kosdaq_title = '코스닥시장상장법인'


def _load_pl(year: int, quarter: int, cpl: bool, con: bool, corps: Dict[str, Corp]):
    type = 'CPL' if cpl else 'PL'
    filename = get_filename(year, quarter, type, con)

    # print(f', Reading {filename}')
    # Sales, Net-Income
    with open('dart-data/' + filename, 'r', encoding='utf-8') as fin:
        fields = fin.readline().split('\t')
        # print(f'Reading {filename}, fields: {len(fields)}')
        value_index = 12
        if len(fields[12].strip()) == 0:
            value_index = 13                
                
        for line in fin:
            data = line.split('\t')
            type = data[3].strip()
            field = data[11].strip()
            field = re.sub('[^()가-힣]', '', field)
            field_code = data[10].strip()

            value = data[value_index].strip()
            stock = data[1][1:-1]

            if type == _dart_kosdaq_title or type == _dart_kospi_title:
                name = data[2].strip()
                market = Market.KOSDAQ if type == _dart_kosdaq_title else Market.KOSPI
                
                if stock not in corps:
                    corps[stock] = Corp(name, stock, market, year, quarter)
                c = corps[stock]

                if value is None or len(value) == 0:
                    continue
                
                try:
                    value = int(value.replace(',', ''))
                    if field == '당기순이익' or field == '당기순이익(손실)' or field == '분기순이익' or field == '분기순이익(손실)' or field_code == _dart_code_net_income1 or field_code == _dart_code_net_income2:
                        c.net_income = value
                    elif field == '매출액' or field == "매출" or field == '수익(매출액)' or field_code == _dart_code_sales1 or field_code == _dart_code_sales2:
                        c.sales = value
                    elif field == '매출원가' or field_code == _dart_code_sales_cost1 or field_code == _dart_code_sales_cost2:
                        c.sales_cost = value
                    elif field == '영업이익(손실)' or field == '영업이익' or field == '영업손실' or field_code == _dart_code_profit1:
                        c.profit = value
                except ValueError:
                    traceback.print_stack()
                    print('Invalid', c.name, field, value)
                    # del corps[stock]


def _load_cf(year: int, quarter: int, con: bool, corps):
    filename = get_filename(year, quarter, 'CF', con)
    with open('dart-data/' + filename, 'r', encoding='utf-8') as fin:    
        fields = fin.readline().split('\t')
        # print(f'Reading {filename}, fields: {len(fields)}')
        value_index = 12
        if len(fields[12].strip()) == 0:
            value_index = 13     
        for line in fin:
            data = line.split('\t')
            type = data[3].strip()
            field = data[11].strip().replace("'", '').replace('"', '')
            field = re.sub('[^()가-힣]', '', field)
            field_code = data[10].strip()
            stock = data[1][1:-1].strip()
            value = data[value_index].strip()

            if type == _dart_kosdaq_title or type == _dart_kospi_title:            
                name = data[2].strip()
                market = Market.KOSDAQ if type == _dart_kosdaq_title else Market.KOSPI
                
                if stock not in corps:
                    corps[stock] = Corp(name, stock, market, year, quarter)
                c = corps[stock]

                if value is None or len(value) == 0:
                    continue
                value = int(value.replace(',', ''))
                try:
                    if field == '영업활동현금흐름' or field == '영업활동으로인한현금흐름' or field_code == _dart_code_cash_flow1 or field_code == _dart_code_cash_flow2:
                        c.cash_flow = value
                    # elif field == '무형자산의취득' or field_code == _dart_code_capex1:
                    elif field == '무형자산의취득' or field_code == _dart_code_capex1 or field_code == _dart_code_capex2:
                        if c.capex_intangible is not None:
                            print('Capex-Intangible is already set:', c.stock, c.name, c.capex_intangible, value)
                        c.capex_intangible == value
                    # elif field == '유형자산의취득' or field_code == _dart_code_capex2:
                    elif field == '유형자산의취득' or field_code == _dart_code_capex3 or field_code == _dart_code_capex4:
                        if c.capex_property is not None:
                            print('Capex-Property is already set:', c.stock, c.name, c.capex_property, value)
                        c.capex_property == value
                except ValueError:
                    traceback.print_stack()
                    print('Invalid', c.name, field, value)
                    # del corps[stock]


def _load_bs(year: int, quarter: int, con: bool, corps: Dict[str, Corp]):        
    filename = get_filename(year, quarter, 'BS', con)
    with open('dart-data/' + filename, 'r', encoding='utf-8') as fin:    
        fields = fin.readline().split('\t')
        # print(f'Reading {filename}, fields: {len(fields)}')
        value_index = 12
        if len(fields[12].strip()) == 0:
            value_index = 13        
        for line in fin:
            data = line.split('\t')
            type = data[3].strip()
            field = data[11].strip()
            field_code = data[10].strip()
            stock = data[1][1:-1]
            value = data[value_index].strip()

            if type == _dart_kosdaq_title or type == _dart_kospi_title:
                if stock not in corps:
                    continue

                c = corps[stock]

                if value is None or len(value) == 0:
                    continue
                
                try:
                    value = int(value.replace(',', ''))
                    if field == '자산총계' or field_code == _dart_code_assets1 or field_code == _dart_code_assets2:
                        c.assets = value
                    if field == '자본총계' or field_code == _dart_code_equity1 or field_code == _dart_code_equity2:
                        c.equity = value
                    elif field == '부채총계' or field_code == _dart_code_liabilities1 or field_code == _dart_code_liabilities2:
                        c.liabilities = value
                    #if field == '자본총계':
                    #    c.book_value = value
                except ValueError:
                    traceback.print_stack()
                    print('Invalid', filename, c.name, field, value)
                    # del corps[stock]

import sys
def _load_ce(year: int, quarter: int, con: bool, corps: Dict[str, Corp]):        
    filename = get_filename(year, quarter, 'CE', con)
    with open('dart-data/' + filename, 'r', encoding='utf-8') as fin:    
        fields = fin.readline().split('\t')
        # print(f'Reading {filename}, fields: {len(fields)}')
        value_index = 12
        for line in fin:
            data = line.split('\t')
            type = data[3].strip()
            field = data[11].strip()
            field_code = data[10].strip()
            stock = data[1][1:-1]
            value = data[value_index].strip()

            if type == _dart_kosdaq_title or type == _dart_kospi_title:
                if stock not in corps:
                    continue

                c = corps[stock]

                if value is None or len(value) == 0:
                    continue
                
                try:
                    value = int(value.replace(',', ''))
                    if field_code == _dart_code_euqity_issue1 or field_code == _dart_code_euqity_issue2:
                        c.equity_issue = value
                except ValueError:
                    pass


def _load_shares(filename, corps):
    with open('dart-data/' + filename, 'r', encoding='utf-8') as fin:    
        # print(f'Reading {filename}')        
        for line in fin:
            data = line.split(',')
            stock = data[0].strip().replace('"', '')
            market = data[2].strip().lower().replace('"', '')

            if market != 'kospi' and market != 'kosdaq':
                continue

            if stock not in corps:
                continue

            c = corps[stock]
            try:
                c.price = int(data[4].strip().replace('"', ''))
                c.shares = int(data[-1].strip().replace('"', ''))
            except ValueError:
                traceback.print_stack()
                print('Invalid', filename, c.name, data)
                continue


def get_filename(year, quarter, type, c):
    cs = '-c' if c else ''
    filename = f'{year}-{quarter}Q-{type}{cs}.txt'
    return filename


def _load_data(corps, year, quarter, type, c=False):
    if type == 'PL':    _load_pl(year, quarter, False, c, corps)
    if type == 'CPL':   _load_pl(year, quarter, True, c, corps)
    if type == 'CF':    _load_cf(year, quarter, c, corps)
    if type == 'BS':    _load_bs(year, quarter, c, corps)
    if type == 'CE':    _load_ce(year, quarter, c, corps)


def load_dart_data(year: int, quarter: int) -> List[Corp]:
    corps           : Dict[str, Corp] = {}

    '''
    cache_filename = _cache_filename(year, quarter)
    try:
        if os.path.exists(cache_filename):
            with open(cache_filename, 'r', encoding='utf-8') as fin:
                return json.load(fin)
    except Exception as ex:
        print(ex)
    '''

    try:
        # 손익계산서(연결)
        _load_data(corps, year, quarter, 'PL', False)    
        _load_data(corps, year, quarter, 'PL', True)

        # 포괄손익계산서(연결)
        _load_data(corps, year, quarter, 'CPL', False)    
        _load_data(corps, year, quarter, 'CPL', True)
        
        # 현금흐름표(연결)
        _load_data(corps, year, quarter, 'CF', False)    
        _load_data(corps, year, quarter, 'CF', True)

        # 재무상태표(연결)
        _load_data(corps, year, quarter, 'BS', False)
        _load_data(corps, year, quarter, 'BS', True)

        # 자본변동
        _load_data(corps, year, quarter, 'CE', False)
        _load_data(corps, year, quarter, 'CE', True)

        # 시가총액, 상장주식수
        filename = f'{year}-{quarter}Q-Stocks.csv'
        _load_shares(filename, corps)

        '''
        with open(f'{year}-{quarter}.log', 'w', encoding='utf-8') as fout:
            for corp in corps.values():
                asdict = corp.__dict__
                valid = True
                # print(asdict)
                for v in asdict.values():
                    if v is None:
                        valid = False
                if not valid:
                    corps_invalid[corp.stock] = corp
                    
                    delkeys = []
                    for k in asdict.keys():
                        if k != 'name' and asdict[k] is not None:
                            delkeys.append(k)                            
                    for k in delkeys:
                        del asdict[k]
                    
                    log = json.dumps(asdict, indent=4, ensure_ascii=False)
                    # print(log)
                    fout.write(log)
                    pass
                
        for stock in corps_invalid:
            del corps[stock]
        # print(f'Invalid: {year} {quarter}Q {len(corps_invalid)}')

        #for c in ret:
        #    c.cal_indicators()
        '''

        ret = list(corps.values())
        return ret
    except Exception as ex:
        # traceback.print_stack()
        # print(ex)
        # print(traceback.format_exc())
        return None


if __name__ == '__main__':
    for year in range(2016, 2021):
        for quarter in range(1, 5):
            corps = load_dart_data(year, quarter)
            print(year, quarter, len(corps))
            print()
    
    year = 2021
    for quarter in range(1, 4):
        corps = load_dart_data(year, quarter)
        print(year, quarter, len(corps))
        print()
    
    
'''
year = 2021
corps = load_corps(year, 1)
print(f'Coporations with full data: {len(corps)}')




# Cash-Flow
type = 'CF'
filename = f'{year}-{quarter}Q-{type}.txt'


# Shares



for corp in corps.values():
    asdict = corp.__dict__
    valid = True
    for v in asdict.values():
        if v is None:
            valid = False
    if not valid:
        print(corp.__dict__)
'''