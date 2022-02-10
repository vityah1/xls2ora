import time
import socket
import datetime
import requests
import os
import json
from os import path
import msvcrt
import re
import __main__ as main

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

# from selenium.webdriver.common.action_chains import ActionChains
len_line=size = os.get_terminal_size()[0]

def getip():
    hostname = socket.gethostname()
    return socket.gethostbyname(hostname)


local_ip = getip()
username = os.environ.get("USERNAME")
nm = os.path.basename(main.__file__).replace(".py", "")
curdir = os.getcwd()


def file2arr(filename=None, sep=",")->list:
    """read file to array with separator"""
    if path.exists(filename):
        myLog(f"Файл {filename} з вхідними даними знайдений. Продовжуємо...",1)
        myarr = []
        with open(filename) as f:
            for r in f:
                if sep:
                    myarr.append(r.strip().split(sep)) if r.strip() else None
                else:
                    myarr.append(r.strip()) if r.strip() else None
        return myarr
    else:
        ss=f"file2arr: Файл {filename} з вхідними даними НЕ знайдений."
        myLog(ss,1)
        sendicqmsg(ss)
        return []


def chkfl(fl):
    """check file stats for compare version exe file"""
    if local_ip!='10.9.19.21':
        try:
            (mode, ino, dev, nlink, uid, gid, size, atime, mtime, ctime) = os.stat(fl)
            return str(time.ctime(os.path.getmtime(fl))) + " " + str(size)
        except Exception as e:
            myLog(f"Error. Can not get stat about {fl}. {e} ")
            return "v.unknown"

def ora_sql(sql):
    data = {
        "action": "sql",
        "sql": sql,
        "src": nm,
        "username": username,
        "user_ip": local_ip,
    }

    zapyt = f"""http://10.9.19.15:5000/api"""

    headers = {"Content-Type": "application/json; charset=ISO-8859-1"}
    proxies = {"http": "", "https": ""}
    try:
        r = requests.post(zapyt, json=data, headers=headers, proxies=proxies)
        # myLog(r.status_code)
        # myLog(r.text)
    except Exception as e:
        myLog(f"""error {e}""",1)

    res = json.loads(r.text)

    # if "error" in res:
    #     myLog("error api")

    if res["cnt"] < 0:
        return [-1]
        # return [-1]

    return res["result"][0]


def ora_sql2(sql):
    data = {
        "action": "sql",
        "sql": sql,
        "src": nm,
        "username": username,
        "user_ip": local_ip,
    }

    zapyt = f"""http://10.9.19.15:5000/api"""

    headers = {"Content-Type": "application/json; charset=ISO-8859-1"}
    proxies = {"http": "", "https": ""}
    try:
        r = requests.post(zapyt, json=data, headers=headers, proxies=proxies)
        # myLog(r.status_code)
        # myLog(r.text)
        res = json.loads(r.text)
    except Exception as e:
        myLog(f"""error api: {e}""",1)
        res = {"cnt": -1, "result": f"{e}"}

    return res


def decl_log(tin="", cnt="0", decl=nm):
    data = {
        "tin": tin,
        "cnt": cnt,
        "decl": decl,
        "action": "decl_log",
        "src": f"{nm}.py",
        "username": username,
        "user_ip": local_ip,
    }

    url = f"""http://10.9.19.15:5000/api"""

    headers = {"Content-Type": "application/json; charset=ISO-8859-1"}
    proxies = {"http": "", "https": ""}
    try:
        r = requests.post(url, json=data, headers=headers, proxies=proxies)
        myLog(r.status_code)
        myLog(r.text)
    except Exception as e:
        # winsound.Beep(sound_info, sound_info_time)
        myLog(f"""error {e}""",1)


def sdatetime(dt=None):
    if dt is None:
        return datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    else:
        return dt.strftime("%d.%m.%Y %H:%M:%S")


def myLog(txt="",arg=0):
    """пишем в лог файл. Якщо параметр то друкуємо на екрані"""
    error=0
    if arg<2:
        if txt == "BEGIN":
            txt = f"\n{txt}"
        elif txt == "END":
            txt = f"{txt}\n"
        else:
            txt = datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S") + f" {txt}"

        with open(f"{nm}.log", "a") as f:
            f.write(txt + "\n")
    
    if arg>0:
        # друкуємо на всю ширину консолі
        if len(txt)>=len_line:
            txt=txt[:len_line-1]
        else:
            txt+=" "*(len_line-len(txt)-1)
        colb=""
        colend=""
        if re.search(r'помилка|error|не знайдено|not found',txt,re.I):
            error=1
            txt+="\n"
            colb=bcolors.FAIL
            colend=bcolors.ENDC

        if len(txt)>=len_line:
            print(f"{colb}{txt}{colend}",end="\r")
        else:
            print(f"{colb}{txt}{colend}",end="\r")
        if error:
            mywait(10)

def print1(txt):
        if len(txt)>=len_line:
            print(txt[:len_line-1],end="\r")
        else:
            print(txt+" "*(len_line-len(txt)-1),end="\r")


def sendicqmsg(to=1001,msg=""):
    """send message to admin [icq:1001] by local icq server"""
    if local_ip=='10.9.19.21':
        return ""
    data = {
        "to": to,
        "msg": f"""{nm}:\n{msg}\npath: [{curdir}]\nusername: [{username}], user_ip: [{local_ip}]""",
        "from": "3",
    }
    url = f"""http://10.9.19.9:8080/siq/cgi-bin/sendicq_ole.cgi"""

    # headers={'Content-Type':'text/html; charset=ISO-8859-1'}
    headers = {"Content-Type": "text/html; charset=windows-1251"}
    proxies = {"http": "", "https": ""}
    try:
        r = requests.get(url, params=data, headers=headers, proxies=proxies)
        if r.status_code == 200:
            myLog("icq message send...")
        else:
            myLog(f"""error send icq msg. status_code: {r.status_code}""")
    except:
        myLog("Error send icq message")


def sec2hours(ss=None):
    """convert secunds to days hours:minutes:secunds"""
    if ss == None:
        return ""
    try:
        ss = int(ss)
    except:
        return ""
    if ss > 0:
        return datetime.timedelta(seconds=ss)
        # return time.strftime('%H:%M:%S', time.gmtime(ss))
        # return "%d:%02d:%02d" % (((ss // 3600)) % 24, (ss // 60) % 60, ss % 60)
    else:
        return ""


def check_work_time():
    """Перевірка чи робочий час"""

    currtime = datetime.datetime.now().strftime("%H:%M")
    if currtime >= "07:00" and currtime <= "22:59":
        pass
    elif local_ip=='10.9.19.21':
        pass    
    else:
        ss=f"""ini_driver: Занадто пізно {currtime} для роботи. час відпочивати...\n"""
        myLog(ss,1)
        sendicqmsg(ss)
        # os._exit(1)
        return False
    return True


def mywait(total_wait=0):
    """Чекаємо задану к-сть секунд з реагуванням на натиснуту клавішу. Виходимо або продовжуємо"""
    
    myLog(f"\nmywait=>({total_wait})")
    print("\nНатисніть q для ВИХОДУ або любу клавішу для продовження\n")
    t = 0
    k = ""
    while True:
        # print ("Doing a function")
        if msvcrt.kbhit():
            k = msvcrt.getch()
            print("\nНатиснута клавіша: %s" % k)
            if k == b"q":
                myLog("\nВИХОДИМО...\n")
                os._exit(1)
            else:
                break
        time.sleep(1)
        t += 1
        print(f"""Чекаємо: {t} сек з {total_wait} cек""", end="\r")
        if t >= total_wait and total_wait > 0:
            break
    myLog("\nПродовжуємо після очікування\n")
    return k

