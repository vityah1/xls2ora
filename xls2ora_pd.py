"""Utility for load [xls|xlsx|csv|html] to oracle table v.0.0.3

Usage: xls2ora.exe file.ext|file.json

file.ext [xls|xlsx|csv|html]

Example config file:
* - required fields

xls2ora.json =>
{
*"table_in":"shtat.shtat_reports",
*"fields_in":"pib,time_inout,rdate",
"file_in":"file.ext", * if arg file.json
"cols":[2,3,"&filename"],
"format":"html|xls|csv",
"truncate":"Y|n",
"delete":" rdate=\"&filename\"",
"required_col":3,
"types":{6:"float",7:"float",8:"float",9:"float",10:"float",11:"float",12:"float"}
}        
"""

import os
import sys
import time
import pandas as pd
import requests
from os import path
import json
import traceback
from datetime import datetime
from transliterate import translit

from funks import (
    # file2arr,
    sendicqmsg,
    nm,
    decl_log,
    myLog,
    username,
    local_ip,
)

url_api = "http://10.9.19.15:5000/api"
headers = {"Content-Type": "application/json; charset=ISO-8859-1"}
proxies = {"http": "", "https": ""}
valid_formats=['html','xls','xlsx','csv']


def get_columns_name(full_table_in):
    owner,table_in=full_table_in.split('.')
    sql=f"""SELECT column_name,data_type FROM all_tab_cols WHERE UPPER(table_name) = UPPER('{table_in}') and column_name!='ID' 
and UPPER(owner)=UPPER('{owner}')
order by column_id
"""
    cnt,res=request_api({"sql":sql,"action":"sql"})
    fields_arr=[]
    types={}
    for i,r in enumerate(res,1):
        if r[1]=='ID':
            continue
        fields_arr.append(r[0])
        if r[1] in ('NUMBER','INTEGER'):
            types[str(i)]='float'

    fields=','.join(fields_arr)
    return fields,types

def truncate_table(table_in="",delete=""):
    um_delete=""
    if delete:
        um_delete=f" where {delete}".replace('"','\"')
    json={"action":"sql","sql":f"delete from {table_in} {um_delete}"}
    return request_api(json)[0]

def ins_to_ora(data_in=[],table_in='',fields_in=''):
    if not data_in or not table_in or not fields_in:
        return "empty input data"
    fields_in=[f.lower().strip() for f in fields_in.split(",")]
    json_data = {
        "action": "executemany",
        "table": table_in,
        "fields":fields_in,
        "data":data_in,}
    
    return request_api(json_data)[0]
    # url = f"""http://127.0.0.1:5000/api"""

def request_api(json_data):
    
    json_data["src"]= nm
    json_data["username"]= username
    json_data["user_ip"]= local_ip


    try:
        resp=requests.post(url_api,json=json_data, headers=headers,proxies=proxies)
    except Exception as e:
        myLog(f"error request api: {e}",1)
        return -1,[]

    myLog(f"resp.status: {resp.status_code}")
    if resp.status_code != 200:
        myLog(f"error resp.status: {resp.status_code}",1)
        return -1,[]
    else:
        try:
            data = resp.json()
            if data["cnt"]<0:
                myLog(f"""error exec sql: {data['result'][0][0]}""",1)
                return -1,[]
            else:
                myLog(f"sql Ok: {data['cnt']} rows",1)
                return data['cnt'],data['result']
        except Exception as e:
            myLog(f"""error get json: {e}""",1)
            return -1,[]


def main():
    try:
        myLog("BEGIN")
        usage=__doc__
        try:    
            arg = sys.argv[1:][0]
        except:
            print(usage)
            return
            
        if arg in ['/?','--help','-h']:
            print(usage)
            return
        
        myLog(f"Utility for load [xls|xlsx|csv|html] to oracle table\n")
        # usage = "xls2ora.exe {filename} формату [XLS]\n!!!"

        if not path.exists(arg):
            myLog(f"""Input file [{arg}] not exists\n""",1)
            print(usage)
            sendicqmsg(1001,f"""Input file [{arg}] not exists""")
            return

        if arg.find(".json")>-1:
            json_file=arg
        else:
            json_file='xls2ora.json'

        create_table=0
        if not path.exists(json_file):
            myLog(f"""Create table mode\n""",1)
            create_table=1
        else:
            try:
                with open(json_file,"r+") as f:
                    cfg = json.load(f)
            except Exception as e:
                myLog(f"""cfg json file [{json_file}] not valid\n{e}\n""",1)
                print(usage)
                sendicqmsg(1001,f"""cfg json file [{json_file}] not valid\n{e}""")
                return
        
        if create_table:
            # if not find xls2ora.json file
            extention=arg.split(".")[-1]
            table_in=f"""cgi.tmp_{arg.replace(f'.{extention}','')}"""
            fields_in=''
            format=extention
            cols=[1]
            file_in=arg
            filename=os.path.basename(file_in)
            # first_row=1
            truncate="N"
            delete=""
            required_col=0
            types={}
            
            # fields_in=cfg["fields_in"]
        else:
            try:
                table_in=cfg["table_in"]
                fields_in=cfg.get("fields_in","")
                
                if arg.find(".json")>-1:
                    try:
                        file_in=cfg["file_in"]
                    except:
                        err=f'\nNot find key "file_in" in json file, append mode'
                        myLog(err,1)
                        print(err)
                        file_in=arg
                        # sendicqmsg(1001,err)
                        # return
                else:
                    file_in=arg
                
                extention=file_in.split(".")[-1]
                filename=os.path.basename(file_in)
                
                # first_row=cfg.get("first_row",1)
                cols=cfg.get("cols",[])
                format=cfg.get("format",extention)
                truncate=cfg.get("truncate","n")
                # separator=cfg.get("separator",",")
                delete=cfg.get("delete","")
                required_col=cfg.get("required_col",0)
                types=cfg.get("types",{})
            except Exception as e:
                myLog(f"""not valid parameters in xls2ora.json\n""",1)
                print(usage)
                sendicqmsg(1001,f"""not valid parameters in xls2ora.json\n{e}""")
                return
        
        if format not in valid_formats:
            myLog("\nformat not valid...\n",1)
            print(usage)
            return

        # if format == 'xls' and filename.find('.xlsx')>-1:
        #     format='xlsx'

        if not fields_in and create_table!=1:
            fields_in,types=get_columns_name(table_in)

        cols_all='N'
        
        if not cols:
            cols_all='Y'
            for i in range(1,len(fields_in.split(","))+1):
                cols.append(i)
        
        if create_table==1:
            cols_all='Y'
        
        myLog(f"Opening {filename} file...\n",1)

        cnt_rows:int=0
        data:list=[]
        try:
            if format=='html':
                df = pd.read_html(file_in,decimal=',',thousands='.')[0]
            elif format in ('xls','xlsx'):
                df = pd.read_excel(file_in,sheet_name=0)
            elif format=="csv":
                df = pd.read_csv(file_in)
        except Exception as e:
            myLog(f"""error open {file_in} file\n{e}\n""",1)
            sendicqmsg(1001,f"""error {file_in} xls file\n{e}""")
            return                        

        cnt_rows = len(df)
        myLog(f"\n{cnt_rows} rows readed from {filename}\n",1)

        start_time_main = time.perf_counter()


        myLog(f"Load data from {filename}...",1)

        if cols_all=='Y' and not fields_in:
            fields=[]
            fields=[translit(str(cell),'uk', reversed=True).lower().replace('№','npp')[:30].replace('/','_').replace('’','').replace(',','_').replace('.','').replace(' ','_').replace('"','').replace("'",'').replace('(','_').replace(')','_').strip() for cell in df.T.axes[0]]            
            cols=[*range(1,len(fields)+1)]

        if create_table==1:
            maxColumnLenghts = []
            for col in range(len(df.columns)):
                maxColumnLenghts.append(max(df.iloc[:,col].astype(str).apply(len)))
            columns=[]

            columns.append(f"create table {table_in} (")
            for i,col in enumerate(fields,0):
                columns.append(f"{col} varchar2 ({maxColumnLenghts[i]}),") if col!='none' else ""
                
            fields_in=",".join(fields).replace(",none","")
            columns[-1]= columns[-1].replace(",","")
            columns.append(")")
            sql="".join(columns)
            res=request_api({"action":"sql","sql":sql})[0]
            if res<0:
                myLog(f"error create table {table_in}")            

        for i, df_row in df.iterrows():
            row=[]
            try:
                for j in cols:
                    not_required_value=0
                    val=''
                    if str(j).find('&filename')>-1:
                        val=filename.replace(f'.{extention}','')
                    else:
                        val=df_row[j-1]
                    if required_col==j-1 and pd.isnull(val):
                        not_required_value=1
                        break
                    if pd.isnull(val) and types.get(str(j))=='float':
                        val=0
                    elif isinstance(val,(int,float)):
                        if types.get(str(j))=='str' and (val==0 or pd.isnull(val)):
                            val=''
                        else:
                            if pd.isnull(val):
                                val=0
                            else:
                                val='{0:.2f}'.format(val).rstrip('0').rstrip('.')                        
                    elif isinstance(val,(str)) and types.get(str(j))=='float':
                        val=float(val.replace(',','.'))
                    elif isinstance(val,datetime):
                        if pd.isnull(val):
                            val=''
                        else:                        
                        # row.append(val.strftime('%d.%m.%Y %H:%M:%S'))
                            val=val.strftime('%d.%m.%Y')
                    
                    # elif pd.isnull(val) and isinstance(val,(str)):
                    elif pd.isnull(val):
                        val=''
                    
                    if not isinstance(val,(str)):
                        if types.get(str(j))!='float':
                            val=str(val)
                    
                    # if not isinstance(val,(str)):
                    #     print(f"\nj: {j}, val: {val}, type not STR, type: {type(val)}\n")
                    
                    row.append(val)
                if not row:
                    break
                if not_required_value:
                    break
                
                data.append(row)
                myLog(f"row: {i}",2)
            except:
                pass
       
        myLog(f"\n{len(data)} rows prepeared for load to {table_in}\n",1)
        if truncate=="Y":
            myLog(f"\ntruncate {table_in}\n",1)
            if truncate_table(table_in=table_in)<0:
                return
        
        if delete:
            if delete.find('&filename')>-1:
                delete=delete.replace('&filename',filename.replace('.xls',''))
            myLog(f"delete {table_in} where {delete}",1)
            if truncate_table(table_in=table_in,delete=delete)<0:
                return            

        myLog(f"send data to {table_in}...",1)
        res = ins_to_ora(data_in=data,table_in=table_in,fields_in=fields_in)
        end = time.perf_counter()

        # total_time = sec2hours(sec)

        msg = f"""{filename} -> {table_in}, loaded rows: [{res}], total time: {end-start_time_main:0.7f} s\nresult: {res}"""
        myLog(msg,1)
        sendicqmsg(1001,msg)
        myLog("END")  
        decl_log(tin="", cnt=cnt_rows, decl=nm)  
    except:
        e = traceback.format_exc()
        myLog(e,1)
        sendicqmsg(1001,e)

if __name__ == "__main__":
    main()