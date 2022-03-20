import os, sys
import platform
import shutil
import subprocess

quarter = {
    '1분기보고서': '1Q',
    '반기보고서':  '2Q',
    '3분기보고서': '3Q',
    '사업보고서':  '4Q'
}

types = {
    '현금흐름표'   : 'CF',
    '손익계산서'   : 'PL',
    '포괄손익계산서': 'CPL',
    '재무상태표'   : 'BS',
    '자본변동표'   : 'CE'
}

failed_list = []
try:
    os.mkdir('converted')
except FileExistsError:
    pass

files = os.listdir('.')
files.sort()
for f in files:
    if f.endswith('.txt'):

        q = None
        t = None
        year = int(f[:4])
        for key in quarter:
            if f.find(key) != -1:                
                q = quarter[key]                
                break
        for key in types:
            if f.find(key) != -1:
                t = types[key]
                if t == 'PL':
                    if f.find('포괄손익계산서') != -1:
                        t = 'CPL'
                break
        
        if q == None or t == None:
            continue

        print(f'Converting {f} ', end='')        
        c = '-c' if f.find('연결') != -1 else ''

        nf = f'{year}-{q}-{t}{c}.txt'
        print(f'to {nf}')
        failed = False
        sysinfo = platform.system().lower()
        if sysinfo.find('windows') != -1:
            subprocess.run(f'powershell "Get-Content {f} | Set-Content -Encoding utf8 {nf}"', shell=True)            
        elif sysinfo.find('linux') != -1:
            pass      
        '''
        with open(f, 'r', encoding='euc-kr') as fin:
            with open(nf, 'w', encoding='utf8') as fout:
                try:
                    fout.write(fin.read())
                except:
                    failed = True
                    failed_list.append(f)
        '''
        if not failed:
            s = os.path.getsize(f)
            ns = os.path.getsize(nf)
            shutil.move(f, f'./converted/{f}')
        else:
            os.remove(nf)
    elif f.endswith('Stocks.csv'):
        with open(f, 'rb') as fin:
            # Skip UTF-8 file
            bom = fin.read(3)
            if bom[0] == 0xEF and bom[1] == 0xBB and bom[3] == 0xBF:
                continue

        print(f'Converting {f}')
        sysinfo = platform.system().lower()
        if sysinfo.find('windows') != -1:
            subprocess.run(f'powershell "Get-Content {f} | Set-Content -Encoding utf8 {f}.utf8"', shell=True)            
            shutil.move(f'{f}.utf8', f)
        elif sysinfo.find('linux') != -1:
            pass   
    