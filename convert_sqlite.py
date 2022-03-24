
from typing import Dict, List

import pandas as pd
import sqlite3
import corp
import krx
import sys, os


_table_name = 'krx'

_indicator_sql = [

    f'UPDATE {_table_name} SET market_cap=price*shares',                    # Market Cap
    f'UPDATE {_table_name} SET book_value=assets-liabilities',              # Book Value
    f'UPDATE {_table_name} SET per=market_cap/net_income',                  # PER
    f'UPDATE {_table_name} SET pbr=market_cap/book_value',                  # PBR
    f'UPDATE {_table_name} SET psr=market_cap/sales',                       # PSR    
    f'UPDATE {_table_name} SET pfcr=market_cap/(cash_flow-capex)',          # PFCR
    f'UPDATE {_table_name} SET iper=1/per',                                 # iPER
    f'UPDATE {_table_name} SET ipbr=1/pbr',                                 # iPBR
    f'UPDATE {_table_name} SET ipsr=1/psr',                                 # iPSR    
    f'UPDATE {_table_name} SET ipfcr=1/pfcr',                               # iPFCR
    f'UPDATE {_table_name} SET roa=net_income/(equity+liabilities)',        # ROA
    f'UPDATE {_table_name} SET roa=net_income/equity',                      # ROE
    f'UPDATE {_table_name} SET sales_profit=sales-sales_cost',              # Sales Profit (매출총이익)
    f'UPDATE {_table_name} SET gpa=sales_profit/book_value',                # GP/A
    f'UPDATE {_table_name} SET profit_growth_qoq=profit/(SELECT profit FROM {_table_name} old WHERE old.stock={_table_name}.stock AND old.year={_table_name}.year AND old.quarter={_table_name}.quarter-1) WHERE quarter > 1',          # QoQ Profit Growth (2~4Q)
    f'UPDATE {_table_name} SET profit_growth_qoq=profit/(SELECT profit FROM {_table_name} old WHERE old.stock={_table_name}.stock AND old.year={_table_name}.year-1 AND old.quarter=4) WHERE quarter = 1',                              # QoQ Profit Growth (1Q)
    f'UPDATE {_table_name} SET profit_growth_yoy=profit/(SELECT profit FROM {_table_name} old WHERE old.stock={_table_name}.stock AND old.year={_table_name}.year-1 AND old.quarter={_table_name}.quarter) WHERE quarter = 1',          # YoY Profit Growth
    f'UPDATE {_table_name} SET net_income_growth_qoq=net_income/(SELECT net_income FROM {_table_name} old WHERE old.stock={_table_name}.stock AND old.year={_table_name}.year AND old.quarter={_table_name}.quarter-1) WHERE quarter > 1',  # QoQ Net Income Growth (2~4Q)
    f'UPDATE {_table_name} SET net_income_growth_qoq=net_income/(SELECT net_income FROM {_table_name} old WHERE old.stock={_table_name}.stock AND old.year={_table_name}.year-1 AND old.quarter=4) WHERE quarter = 1',                      # QoQ Net Income Growth (1Q)
    f'UPDATE {_table_name} SET profit_growth_yoy=net_income/(SELECT net_income FROM {_table_name} old WHERE old.stock={_table_name}.stock AND old.year={_table_name}.year-1 AND old.quarter={_table_name}.quarter) WHERE quarter = 1',  # YoY Net Income Growth
    f'UPDATE {_table_name} SET assets_growth_qoq=assets/(SELECT assets FROM {_table_name} old WHERE old.stock={_table_name}.stock AND old.year={_table_name}.year AND old.quarter={_table_name}.quarter-1) WHERE quarter > 1',          # QoQ Assets Growth (2~4Q)
    f'UPDATE {_table_name} SET assets_growth_qoq=assets/(SELECT assets FROM {_table_name} old WHERE old.stock={_table_name}.stock AND old.year={_table_name}.year-1 AND old.quarter=4) WHERE quarter = 1',                              # QoQ Assets Growth (1Q)
    f'UPDATE {_table_name} SET fscore_k=0',     # F-Score
    f'UPDATE {_table_name} SET fscore_k=fscore_k+1 WHERE equity_issue>0',     # F-Score
    f'UPDATE {_table_name} SET fscore_k=fscore_k+1 WHERE net_income>0',       # F-Score
    f'UPDATE {_table_name} SET fscore_k=fscore_k+1 WHERE cash_flow>0',        # F-Score
]

'''
    
        self.asset_growth_qoq = self.assets / qoq.assets
        
        self.fscore_k = 0
        if self.equity_issue > 0:
            self.fscore_k += 1
        if self.net_income > 0:
            self.fscore_k += 1
        if self.cash_flow > 0:
            self.fscore_k += 1
'''


def cal():
    sqlite_filename = 'krx.db'

    if not os.path.exists(sqlite_filename):
        print(f'{sqlite_filename} does not exist.')
        sys.exit()

    conn = sqlite3.connect(sqlite_filename)
    cur = conn.cursor()
    for sql in _indicator_sql:
        calculate_indicators(sql, cur)

    cur.close()
    conn.commit()
    conn.close()


def prep(begin_year, begin_quarter, end_year, end_quarter):
    corps = None
    year = begin_year
    quarter = begin_quarter
    sqlite_filename = 'krx.db'

    if os.path.exists(sqlite_filename):
        os.remove(sqlite_filename)

    conn = sqlite3.connect(sqlite_filename)
    tlist = []
    clist: List[corp.Corp] = []
    while year * 10 + quarter < end_year * 10 + end_quarter:    
        print(f'Processing {year}-{quarter}Q', end='')

        corps = krx.load_dart_data(year, quarter)

        if corps is not None and len(corps) > 0:
            clist.append(corps)
            tlist.append((year, quarter))
            print(f', loaded {len(corps)} corporations.', end='')
        print()
        
        quarter += 1
        if quarter == 5:
            quarter = 1
            year += 1

    dflist = []
    for i, t in enumerate(tlist):
        corps = clist[i]

        '''
        qoq_map = {}
        yoy_map = {}
        corps_map = {}

        for c in corps:
            corps_map[c.stock] = c
        for c in clist[i - 1]:
            qoq_map[c.stock] = c
        for c in clist[i - 4]:
            yoy_map[c.stock] = c
        
        if i > 4:
            for c in corps:
                qoq = qoq_map[c.stock]
                yoy = yoy_map[c.stock]
                c.cal_indicators(qoq, yoy)
        '''

        d = pd.DataFrame([c.__dict__ for c in corps], index=[c.stock for c in corps])
        d = d.drop(['stock'], axis=1)
        d.index.name = 'stock'
        d['year'] = t[0]
        d['quarter'] = t[1]
        dflist.append(d)
    
    
    d = pd.concat(dflist)
    d.to_sql(_table_name, conn, if_exists='append')
    cur = conn.cursor()
    cur.execute(f'CREATE INDEX index_year_quarter_stock on {_table_name}(year, quarter, stock)')
    cur.execute(f'CREATE INDEX index_year_quarter on {_table_name}(year, quarter)')
    cur.execute(f'CREATE UNIQUE INDEX index_key on {_table_name}(year, quarter, stock)')

    cur.close()
    conn.close()


def calculate_indicators(sql: str, cursor: sqlite3.Cursor):
    print(sql)
    cursor.execute(sql)
    


if __name__ == '__main__':
    # prep(int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4]))
    cal()