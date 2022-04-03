
from datetime import datetime
from typing import Dict, List, Tuple

import pandas as pd
import sqlite3
import corp
import krx
import sys, os
import numpy as np

import multiprocessing

_sqlite_filename = 'halq.db'
_table_name = 'krx'

_indicator_sql = [
    f'UPDATE {_table_name} SET fscore_k=0',
    f'UPDATE {_table_name} SET fscore_k=fscore_k+1 WHERE equity_issue > 0',
    f'UPDATE {_table_name} SET fscore_k=fscore_k+1 WHERE net_income > 0',
    f'UPDATE {_table_name} SET fscore_k=fscore_k+1 WHERE cash_flow > 0',

    f'UPDATE {_table_name} SET qoq_profit=    (SELECT profit     FROM {_table_name} old WHERE old.stock={_table_name}.stock AND old.year={_table_name}.year   AND old.quarter={_table_name}.quarter-1) WHERE quarter > 1',            # QoQ Profit (2~4Q)
    f'UPDATE {_table_name} SET qoq_profit=    (SELECT profit     FROM {_table_name} old WHERE old.stock={_table_name}.stock AND old.year={_table_name}.year-1 AND old.quarter=4)                       WHERE quarter = 1',            # QoQ Profit (1Q)
    f'UPDATE {_table_name} SET qoq_net_income=(SELECT net_income FROM {_table_name} old WHERE old.stock={_table_name}.stock AND old.year={_table_name}.year   AND old.quarter={_table_name}.quarter-1) WHERE quarter > 1',            # QoQ Net Income (2~4Q)
    f'UPDATE {_table_name} SET qoq_net_income=(SELECT net_income FROM {_table_name} old WHERE old.stock={_table_name}.stock AND old.year={_table_name}.year-1 AND old.quarter=4)                       WHERE quarter = 1',            # QoQ Net Income (1Q)
    f'UPDATE {_table_name} SET qoq_assets=    (SELECT assets     FROM {_table_name} old WHERE old.stock={_table_name}.stock AND old.year={_table_name}.year   AND old.quarter={_table_name}.quarter-1) WHERE quarter > 1',            # QoQ Net Assets (2~4Q)
    f'UPDATE {_table_name} SET qoq_assets=    (SELECT assets     FROM {_table_name} old WHERE old.stock={_table_name}.stock AND old.year={_table_name}.year-1 AND old.quarter=4)                       WHERE quarter = 1',            # QoQ Net Assets (1Q)
    f'UPDATE {_table_name} SET qoq_book_value=(SELECT book_value FROM {_table_name} old WHERE old.stock={_table_name}.stock AND old.year={_table_name}.year   AND old.quarter={_table_name}.quarter-1) WHERE quarter > 1',            # QoQ Net Book Value (2~4Q)
    f'UPDATE {_table_name} SET qoq_book_value=(SELECT book_value FROM {_table_name} old WHERE old.stock={_table_name}.stock AND old.year={_table_name}.year-1 AND old.quarter=4)                       WHERE quarter = 1',            # QoQ Net Book Value (1Q)    
    
    f'UPDATE {_table_name} SET yoy_profit=    (SELECT profit     FROM {_table_name} old WHERE old.stock={_table_name}.stock AND old.year={_table_name}.year-1 AND old.quarter={_table_name}.quarter)',                                # YoY Profit
    f'UPDATE {_table_name} SET yoy_net_income=(SELECT net_income FROM {_table_name} old WHERE old.stock={_table_name}.stock AND old.year={_table_name}.year-1 AND old.quarter={_table_name}.quarter)',                                # YoY Net Income
    
    f'UPDATE {_table_name} SET profit_growth_qoq    =(profit-qoq_profit)/qoq_profit',               # QoQ Profit Growth
    f'UPDATE {_table_name} SET net_income_growth_qoq=(net_income-qoq_net_income)/qoq_net_income',   # QoQ Net Income Growth
    f'UPDATE {_table_name} SET assets_growth_qoq    =(assets-qoq_assets)/qoq_assets',               # QoQ Assets Growth
    f'UPDATE {_table_name} SET book_value_growth_qoq=(book_value-qoq_book_value)/qoq_book_value',   # QoQ Book Value Growth

    f'UPDATE {_table_name} SET profit_growth_yoy    =(profit-yoy_profit)/yoy_profit',               # YoY Profit Growth
    f'UPDATE {_table_name} SET net_income_growth_yoy=(net_income-yoy_net_income)/yoy_net_income',   # YoY Net Income Growth    
]


def cal():
    if not os.path.exists(_sqlite_filename):
        print(f'{_sqlite_filename} does not exist.')
        sys.exit()

    conn = sqlite3.connect(_sqlite_filename)
    cur = conn.cursor()
    for sql in _indicator_sql:
        calculate_indicators(sql, cur)

    cur.close()
    conn.commit()
    conn.close()


def prep(begin_year: int, begin_quarter: int, end_year: int, end_quarter: int) -> None:
    corps = None
    year = begin_year
    quarter = begin_quarter

    # Delete SQLite database file if exists
    if os.path.exists(_sqlite_filename):
        print(f'Removing file {_sqlite_filename}')
        os.remove(_sqlite_filename)

    tlist: List[List[Tuple[int, int]]] = []     # year, quarter
    clist: List[List[corp.Corp]]       = []     # coporations

    while year * 10 + quarter <= end_year * 10 + end_quarter:    
        # Load corporations
        if krx.data_exists(year, quarter):        
            tlist.append((year, quarter))

        # Next quarter
        quarter += 1
        if quarter == 5:
            quarter = 1
            year += 1
    
    if len(tlist) == 0:
        return

    # Build DataFrame
    print(f'Loading corporations from {tlist[0][0]}-{tlist[0][1]}Q to {tlist[-1][0]}-{tlist[-1][1]} ')
    num_cores = multiprocessing.cpu_count() * 2
    with multiprocessing.Pool(num_cores) as pool:
        clist = pool.starmap(krx.load_dart_data, tlist)

    dflist = []
    for i, t in enumerate(tlist):
        corps = clist[i]
        d = pd.DataFrame([c.__dict__ for c in corps], index=[c.stock for c in corps])
        d['year'] = t[0]
        d['quarter'] = t[1]
        dflist.append(d)
    
    # Caculcate simple indicators
    print('Calculating simple indicators')
    df = pd.concat(dflist)
    df['capex'] = df['capex_intangible'] + df['capex_property']        # capex
    df['market_cap'] = df['price'] * df['shares']                      # Market Cap
    df['book_value'] = df['assets'] - df['liabilities']                # Book Value
    df['per'] = df['market_cap'] / df['net_income']                    # PER
    df['pbr'] = df['market_cap'] / df['book_value']                    # PBR
    df['psr'] = df['market_cap'] / df['sales']                         # PSR    
    df['pcr'] = df['market_cap'] / df['cash_flow']                     # PCR
    df['fcf'] = df['cash_flow'] - df['capex']                          # FCF
    df['pfcr'] = df['market_cap'] / (df['cash_flow'] - df['capex'])    # PFCR
    df['iper'] = 1 / df['per']                                         # iPER
    df['ipbr'] = 1 / df['pbr']                                         # iPBR
    df['ipsr'] = 1 / df['psr']                                         # iPSR    
    df['ipfcr'] = 1 / df['pfcr']                                       # iPFCR
    df['ipcr'] = 1 / df['pcr']                                         # iPCR
    df['roe'] = df['net_income'] / df['equity']                        # ROE
    df['roa'] = df['net_income'] / df['assets']                        # ROA
    df['sales_profit'] = df['sales'] - df['sales_cost']                # Sales Profit (매출총이익)
    df['gpa'] = df['sales_profit'] / df['book_value']                  # GP/A

  

    # Create and open SQLite database file
    conn = sqlite3.connect(_sqlite_filename)
    conn.row_factory = sqlite3.Row
    df.to_sql(_table_name, conn, if_exists='append')
    cur = conn.cursor()

    cur.execute(f'CREATE INDEX index_year_quarter_stock on {_table_name}(year, quarter, stock)')
    cur.execute(f'CREATE INDEX index_year_quarter on {_table_name}(year, quarter)')
    cur.execute(f'CREATE UNIQUE INDEX index_key on {_table_name}(year, quarter, stock)')

    conn.commit()

    print('Calculating QoQ, YoY indicators')
    for sql in _indicator_sql:
        calculate_indicators(sql, cur)
    conn.commit()


    print('Ranking corporations by some indicators')
    for t in tlist:
        year = t[0]
        quarter = t[1]
        sql = f'WITH  sorted AS (\
                SELECT \
                    year, quarter, stock, \
                    ROW_NUMBER() OVER (ORDER BY iper DESC) as iper_sorted, \
                    ROW_NUMBER() OVER (ORDER BY ipbr DESC) as ipbr_sorted, \
                    ROW_NUMBER() OVER (ORDER BY ipcr DESC) as ipcr_sorted, \
                    ROW_NUMBER() OVER (ORDER BY ipfcr DESC) as ipfcr_sorted, \
                    ROW_NUMBER() OVER (ORDER BY ipsr DESC) AS ipsr_sorted, \
                    ROW_NUMBER() OVER (ORDER BY profit_growth_qoq DESC) AS profit_growth_qoq_sorted, \
                    ROW_NUMBER() OVER (ORDER BY net_income_growth_qoq DESC) AS net_income_growth_qoq_sorted, \
                    ROW_NUMBER() OVER (ORDER BY assets_growth_qoq DESC) AS assets_growth_qoq_sorted, \
                    ROW_NUMBER() OVER (ORDER BY book_value_growth_qoq DESC) AS book_value_growth_qoq_sorted, \
                    ROW_NUMBER() OVER (ORDER BY profit_growth_yoy DESC) AS profit_growth_yoy_sorted, \
                    ROW_NUMBER() OVER (ORDER BY net_income_growth_yoy DESC) AS net_income_growth_yoy_sorted \
                FROM krx \
                WHERE year={year} AND quarter={quarter} \
                ) \
                UPDATE  krx SET \
                    ord_iper=(SELECT iper_sorted FROM sorted WHERE krx.stock=sorted.stock), \
                    ord_ipbr=(SELECT ipbr_sorted FROM sorted WHERE krx.stock=sorted.stock), \
                    ord_ipcr=(SELECT ipcr_sorted FROM sorted WHERE krx.stock=sorted.stock), \
                    ord_ipfcr=(SELECT ipfcr_sorted FROM sorted WHERE krx.stock=sorted.stock), \
                    ord_ipsr=(SELECT ipsr_sorted FROM sorted WHERE krx.stock=sorted.stock), \
                    ord_profit_growth_qoq     =(SELECT profit_growth_qoq_sorted     FROM sorted WHERE krx.stock=sorted.stock), \
                    ord_net_income_growth_qoq =(SELECT net_income_growth_qoq_sorted FROM sorted WHERE krx.stock=sorted.stock), \
                    ord_assets_growth_qoq     =(SELECT assets_growth_qoq_sorted     FROM sorted WHERE krx.stock=sorted.stock), \
                    ord_book_value_growth_qoq =(SELECT book_value_growth_qoq_sorted FROM sorted WHERE krx.stock=sorted.stock), \
                    ord_profit_growth_yoy     =(SELECT profit_growth_yoy_sorted     FROM sorted WHERE krx.stock=sorted.stock), \
                    ord_net_income_growth_yoy =(SELECT net_income_growth_yoy_sorted FROM sorted WHERE krx.stock=sorted.stock) \
                WHERE year={year} AND quarter={quarter} AND stock=(SELECT stock FROM sorted WHERE krx.stock=sorted.stock)'
        cur.execute(sql)        
    conn.commit()
    cur.close()
    conn.close()


def calculate_indicators(sql: str, cursor: sqlite3.Cursor) -> int:
    # print(sql, end='')
    count = cursor.execute(sql).rowcount
    # print(str(count))
    return count


if __name__ == '__main__':
    if len(sys.argv) == 1:
        year_start = 2016
        quarter_start = 1
        now = datetime.now()
        year_end = now.year
        quarter_end = 1
        if now.month > 3:    quarter_end += 1
        if now.month > 6:    quarter_end += 1
        if now.month > 10:   quarter_end += 1
    elif len(sys.argv) == 5:
        year_start = int(sys.argv[1])
        quarter_start = int(sys.argv[2])
        year_end = int(sys.argv[3])
        quarter_end = int(sys.argv[4])
    else:
        print('convert_sqlite.py [start year] [start quarter] [end year] [end quarter]')
        sys.exit(1)

    prep(year_start, quarter_start, year_end, quarter_end)
