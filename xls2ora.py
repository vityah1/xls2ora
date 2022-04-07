"""Utility for load [xls|xlsx|csv|html] file to oracle table v.0.0.5

Usage: xls2ora.exe file.ext|file.json

file.ext [xls|xlsx|csv|html]

Example config file:

xls2ora.json =>
{
"table_in":"shtat.shtat_reports",
"fields_in":"pib,time_inout,rdate",
"file_in":"file.ext", * if arg file.json
"cols":[2,3,"&filename"],
"format":"html|xls|csv",
"truncate":"Y|n",
"delete":" rdate=\"&filename\"",
"required_col":3,
"types":{6:"float",7:"float",8:"float",9:"float",10:"float",11:"float",12:"float"},
"ora_user":"user",
"ora_pwd":"password",
"ora_dsn":"ora_dsn"
}

## Create table mode:
* oracle table will create as {ora_user}.tmp_{file} if {table_in} not set
* oracle columns name wiil get from row with headers

## Only load data:
* oracle columns name will get from oracle table if fields_in not set

## Common:
* cols (array) - load data only from listed numbers of columns
* truncate - delete or not data in table before load
* delete - delete with condition
* &filename - macros for replace
* required_col (array) - if data empty in this column the load will stop
* types (dict) {number of column:"float"} - for correct load float|integer|number data
"""

import os
import sys
import time
import re
from pandas import read_excel,read_csv,read_html,isnull
import cx_Oracle
from os import path
import json
import traceback
from datetime import datetime
# from transliterate import translit

from funks import (
    nm,
    decl_log,
    myLog,
    username,
    local_ip,
)

symbols = (u"абвгдеёжзийклмнопрстуфхцчшщъыьэюяАБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ ієІЄ№/,.\"’'()qwertyuioplkjhgfdsazxcvbnm",
    (*list(u'abvgdee'), 'zh', *list(u'zijklmnoprstuf'), 'kh', 'z', 'ch', 'sh', 'sh', '',
    'y', '', 'e', 'yu','ya', *list(u'ABVGDEE'), 'ZH', 
    *list(u'ZIJKLMNOPRSTUF'), 'KH', 'Z', 'CH', 'SH', 'SH', *list(u'_Y_E'), 'YU', 'YA', '_','i','ye','I','YE','npp','_','_','','','','','_','_',*list(u'qwertyuioplkjhgfdsazxcvbnm')))

coding_dict = {source: dest for source, dest in zip(*symbols)}
translate = lambda x: ''.join([coding_dict[i] for i in x])

valid_formats=['html','xls','xlsx','csv']
cursor=None

def cnn2ora(ora_user='',ora_pwd='',ora_dsn=''):
    try:
        connection = cx_Oracle.connect(user=ora_user, password=ora_pwd,
                                dsn=ora_dsn,
                                #encoding="UTF-8"
                                )
        cursor = connection.cursor()
    except Exception as e:
        myLog(f"error {e}",1)
        cursor=None

    return cursor

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

def bulk_load(data_in=[],table_in='',fields_in=''):
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

def do_ora_cmd(json_data={}):

    global cursor

    if json_data['action']=='executemany':
        cols=[]
        for j,col in enumerate(json_data['data'][0],1):
            cols.append(':'+str(j))
        values=','.join(cols)
        cursor.executemany(f"insert into {json_data['table']} ({','.join(json_data['fields'])}) values ({values})", json_data['data'])
        cnt=cursor.rowcount
        cursor.execute('commit')
        return cnt,[]

    sql=json_data['sql'].strip()
    if re.search(r'^select|^with', sql, flags=re.I):
        try:
            # cursor=db.engine().execute('select sysdate as curdate from dual')
            result = cursor.execute(sql)
        except Exception as e:
            myLog(f'''error exec sql:\n{e}''')
            return -1,[[f'''error: {e}''']]

        result_0 = []
        for el in result:
            result_0.append(list(el))

        cnt = result.rowcount

        try:
            return cnt,result_0
        except Exception as e:
            myLog(f'''error return jsonify:\nerror: [{e}]\nresult_0: {result_0[0]}''')
            return -1,[[f'''error: {e}''']]
    elif re.search(r'^insert|^update|^create|^delete|^merge|^truncate', sql, flags=re.I):
        try:
            cursor.execute(sql)
            cnt = cursor.rowcount
            cursor.execute('commit')
            return cnt,[[f'''Affected {cnt} rows''']]
        except Exception as e:
            myLog(f'''error exec sql:\n{e}''')
            return -1, [[f'''Error: {e}''']]
            # return jsonify({"error":f'''error execute sql. error: {e}'''})
    else:
        myLog(f'''sql: {sql}''')
        return -1, [[f'''Not valid sql''']]


def request_api(json_data):
    global cursor
    
    json_data["src"]= nm
    json_data["username"]= username
    json_data["user_ip"]= local_ip

    if cursor is not None:
        return do_ora_cmd(json_data=json_data)
    return -1,[[]]
    
def main():
    try:
        myLog("BEGIN")
        usage=__doc__
        global cursor
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
                return

        try:
            ora_user=cfg.get('ora_user','cgi')
            ora_pwd=cfg.get('ora_pwd','')
            ora_dsn=cfg.get('ora_dsn','')

            if all([ora_user,ora_pwd,ora_dsn]):
                cursor=cnn2ora(ora_user=ora_user,ora_pwd=ora_pwd,ora_dsn=ora_dsn)        
        except:
            pass
        
        if create_table:
            # if not find xls2ora.json file
            extention=arg.split(".")[-1]
            table_in=f"""{ora_user}.tmp_{arg.replace(f'.{extention}','')}"""
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
                return
        
        if format not in valid_formats:
            myLog("\nformat not valid...\n",1)
            print(usage)
            return

        # if format == 'xls' and filename.find('.xlsx')>-1:
        #     format='xlsx'

        if not fields_in and create_table!=1:
            try:
                myLog(f"fields_in not set so get fields from {table_in}",1)
                fields_in,types=get_columns_name(table_in)
            except:
                myLog("Error. Not set schema in table name",1)
                return

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
                df = read_html(file_in,decimal=',',thousands='.')[0]
            elif format in ('xls','xlsx'):
                df = read_excel(file_in,sheet_name=0)
            elif format=="csv":
                df = read_csv(file_in)
        except Exception as e:
            myLog(f"""error open {file_in} file\n{e}\n""",1)
            return                        

        cnt_rows = len(df)
        myLog(f"\n{cnt_rows} rows readed from {filename}\n",1)

        start_time_main = time.perf_counter()


        myLog(f"Load data from {filename}...",1)

        if cols_all=='Y' and not fields_in:
            fields=[]
            fields=[translate(str(cell).lower()).strip() for cell in df.T.axes[0]]            
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
            myLog(sql,1)
            res=request_api({"action":"sql","sql":sql})[0]
            if res<0:
                myLog(f"error create table {table_in}")
                fields_in,types=get_columns_name(table_in)            

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
                    if required_col==j-1 and isnull(val):
                        not_required_value=1
                        break
                    if isnull(val) and types.get(str(j))=='float':
                        val=0
                    elif isinstance(val,(int,float)):
                        if types.get(str(j))=='str' and (val==0 or isnull(val)):
                            val=''
                        else:
                            if isnull(val):
                                val=0
                            else:
                                val='{0:.2f}'.format(val).rstrip('0').rstrip('.')                        
                    elif isinstance(val,(str)) and types.get(str(j))=='float':
                        val=float(val.replace(',','.'))
                    elif isinstance(val,datetime):
                        if isnull(val):
                            val=''
                        else:                        
                        # row.append(val.strftime('%d.%m.%Y %H:%M:%S'))
                            val=val.strftime('%d.%m.%Y')
                    
                    # elif isnull(val) and isinstance(val,(str)):
                    elif isnull(val):
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
        res = bulk_load(data_in=data,table_in=table_in,fields_in=fields_in)
        end = time.perf_counter()

        # total_time = sec2hours(sec)

        msg = f"""{filename} -> {table_in}, loaded rows: [{res}], total time: {end-start_time_main:0.7f} s\nresult: {res}"""
        myLog(msg,1)
        myLog("END")  
        decl_log(tin="", cnt=cnt_rows, decl=nm)  
    except:
        e = traceback.format_exc()
        myLog(e,1)

if __name__ == "__main__":
    main()