import time
import socket
import datetime
import os
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
        return []


def chkfl(fl):
    """check file stats for compare version exe file"""

    try:
        (mode, ino, dev, nlink, uid, gid, size, atime, mtime, ctime) = os.stat(fl)
        return str(time.ctime(os.path.getmtime(fl))) + " " + str(size)
    except Exception as e:
        myLog(f"Error. Can not get stat about {fl}. {e} ")
        return "v.unknown"

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

