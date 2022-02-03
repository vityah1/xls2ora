"""script for load data from xls file to oracle table
Usage: xls2ora.exe file.xls (формату XLS або HTML)

Example config file:
* - required fields

xls2ora.json =>
{
*"table_in":"shtat.shtat_reports",
*"fields_in":"pib,time_inout,rdate",
"first_row":2,
"cols":[2,3,"&filename"],
"format":"html|xls",
"truncate":"Y|n",
"delete":" rdate=\"&filename\""
}        
"""

import os
# from re import T
import sys
import xlrd, time
import requests
from os import path
# from sys import exit
import json
import traceback
from funks import (
    sendicqmsg,
    nm,
    decl_log,
    myLog,
    sec2hours,
    username,
    local_ip,
)

url_api = "http://10.9.19.15:5000/api"
headers = {"Content-Type": "application/json; charset=ISO-8859-1"}
proxies = {"http": "", "https": ""}


def truncate_table(table_in="",delete=""):
    um_delete=""
    if delete:
        um_delete=f" where {delete}".replace('"','\"')
    json={"action":"sql","sql":f"delete from {table_in} {um_delete}"}
    return request_api(json)

def ins_to_ora(data_in=[],table_in='',fields_in=''):
    if not data_in or not table_in or not fields_in:
        return "empty input data"
    fields_in=[f.lower().strip() for f in fields_in.split(",")]
    json_data = {
        "action": "executemany",
        "table": table_in,
        "fields":fields_in,
        "data":data_in,}
    
    return request_api(json_data)
    # url = f"""http://127.0.0.1:5000/api"""

def request_api(json_data):
    
    json_data["src"]= nm
    json_data["username"]= username
    json_data["user_ip"]= local_ip


    try:
        resp=requests.post(url_api,json=json_data, headers=headers,proxies=proxies)
    except Exception as e:
        myLog(f"error request api: {e}",1)
        return -1

    myLog(f"resp.status: {resp.status_code}")
    if resp.status_code != 200:
        myLog(f"error resp.status: {resp.status_code}",1)
        return -1
    else:
        try:
            data = resp.json()
            if data["cnt"]<0:
                myLog(f"""error exec sql: {data['result'][0][0]}""",1)
                return -1
            else:
                myLog(f"sql Ok: {data['cnt']} rows",1)
                return 1
        except Exception as e:
            myLog(f"""error get json: {e}""",1)
            return -1


def do_xls2ora():
    try:
        myLog("BEGIN")
        usage=__doc__
        try:    
            xls_file = sys.argv[1:][0]
        except:
            print(usage)
            return
            
        if xls_file in ['/?','--help','-h']:
            print(usage)
            return
        
        myLog(f"Programm for load {xls_file} to oracle\n")
        # usage = "xls2ora.exe {xls_file} формату [XLS]\n!!!"

        if not path.exists(xls_file):
            myLog(f"""Input file [{xls_file}] not exists\n""",1)
            print(usage)
            sendicqmsg(1001,f"""Input file [{xls_file}] not exists""")
            return

        filename=os.path.basename(xls_file)
        try:
            with open("xls2ora.json") as f:
                cfg = json.load(f)
        except Exception as e:
            myLog(f"""cfg json file [xls2ora.json] not correct\n""",1)
            print(usage)
            sendicqmsg(1001,f"""cfg json file [xls2ora.json] not correct\n{e}""")
            return

        try:
            table_in=cfg["table_in"]
            fields_in=cfg["fields_in"]
            first_row=cfg.get("first_row",1)
            cols=cfg.get("cols",[])
            format=cfg.get("format","xls")
            truncate=cfg.get("truncate","n")
            delete=cfg.get("delete","")
        except Exception as e:
            myLog(f"""not correct parameters in xls2ora.json\n""",1)
            print(usage)
            sendicqmsg(1001,f"""not correct parameters in xls2ora.json\n{e}""")
            return
        
        if format not in ['html','xls']:
            myLog("\nformat not valid...",1)
            print(usage)
            return

        if not cols:
            for i in range(1,len(fields_in.split(","))+1):
                cols.append(i)
        
        myLog(f"Opening {xls_file} file...\n",1)
        
        if format=='html':
            with open(xls_file,'r',encoding="utf-8") as f:
                s = f.read()
            import bs4
            soup = bs4.BeautifulSoup(s, 'html.parser')
            myLog("Begin parce...",1)

            trs=soup.find_all('tr')
            cnt_rows=len(trs)
            names=["html_table"]
        elif format=='xls':
            try:
                wb = xlrd.open_workbook(xls_file)
            except Exception as e:
                myLog(f"""error open xls file\n{e}\n""",1)
                sendicqmsg(1001,f"""error open xls file\n{e}""")
                return                        
            
            names = wb.sheet_names()
            myLog("The number of worksheets is {0}".format(wb.nsheets),1)
            myLog("Worksheet name(s): {0}".format(names),1)
            ws = wb.sheet_by_index(0)
            cnt_rows = ws.nrows - 1

        myLog(f"{cnt_rows} rows loaded from {xls_file}, sheet: {names[0]}",1)

        start_time_main = time.perf_counter()
        data=[]

        myLog("Get data from EXCEL...",1)
        for i in range(first_row,cnt_rows+1):
            row=[]
            try:
                for j in cols:
                    if str(j).find('&filename')>-1:
                        val=filename.replace('.xls','')
                    else:
                        if format=='xls':
                            val=str(ws.cell(i, j-1).value).strip()
                        elif format=='html':
                            val=trs[i].find_all("td")[j-1].getText().strip()
                    row.append(val)

                data.append(row)
            except:
                pass

       
        if truncate=="Y":
            myLog(f"truncate {table_in}",1)
            if truncate_table(table_in=table_in)<0:
                return
        
        if delete:
            if delete.find('&filename')>-1:
                delete=delete.replace('&filename',filename.replace('.xls',''))
            myLog(f"delete {table_in} where {delete}",1)
            if truncate_table(table_in=table_in,delete=delete)<0:
                return            

        myLog(f"send data to {table_in}...",1)
        res=ins_to_ora(data_in=data,table_in=table_in,fields_in=fields_in)
        end = time.perf_counter()

        # total_time = sec2hours(sec)

        msg = f"""total rows: [{cnt_rows}], total time: {end-start_time_main:0.7f} s\nresult: {res}"""
        myLog(msg,1)
        sendicqmsg(1001,msg)
        myLog("END")  
        decl_log(tin="", cnt=cnt_rows, decl=nm)  
    except:
        e = traceback.format_exc()
        myLog(e,1)
        sendicqmsg(1001,e)

def main():
    do_xls2ora()

if __name__ == "__main__":
    main()