
from typing import Dict, List

import pandas as pd
import sqlite3
import corp
import krx
import sys, os


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
        print(f'Processing {year}-{quarter}Q')

        corps = krx.load_dart_data(year, quarter)
        if corps is not None and len(corps) > 0:
            clist.append(corps)
            tlist.append((year, quarter))        
        
        quarter += 1
        if quarter == 5:
            quarter = 1
            year += 1

    dflist = []
    for i, t in enumerate(tlist):
        corps = clist[i]
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
        
        d = pd.DataFrame([c.__dict__ for c in corps], index=[c.stock for c in corps])
        print(d.head())
        d = d.drop(['stock'], axis=1)
        d.index.name = 'stock'
        d['year'] = t[0]
        d['quarter'] = t[1]
        dflist.append(d)
    
    
    d = pd.concat(dflist)
    d.to_sql('fin', conn, if_exists='append')
    cur = conn.cursor()
    cur.execute('CREATE INDEX index_year_quarter_stock on fin(year, quarter, stock)')
    cur.execute('CREATE INDEX index_year_quarter on fin(year, quarter)')
    cur.execute('CREATE UNIQUE INDEX index_key on fin(year, quarter, stock')

    cur.close()
    conn.close()




if __name__ == '__main__':
    prep(int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4]))