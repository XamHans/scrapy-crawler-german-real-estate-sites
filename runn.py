#!/usr/bin/env python
# -*- coding: utf-8 -*-

from database import DataBase
from scrapy.utils.project import get_project_settings
import scrapy.crawler as crawler
from scrapy.crawler import CrawlerProcess
from twisted.internet import reactor
from datetime import datetime
from Notify import Notify
import time
from datetime import timedelta
import psutil
from twisted.internet import reactor, defer
from twisted.internet.task import deferLater
from twisted.internet.protocol import ProcessProtocol
from demo_crawl.spiders.immoscout import ImmoSpider
from demo_crawl.spiders.immonet import ImmonetSpider
from demo_crawl.spiders.sparkasse import SparkasseSpider
from demo_crawl.spiders.meinestadt import MeineStadtSpider
from demo_crawl.spiders.checkStadt import CheckStadtSpider
import logging
from scrapy.utils.log import configure_logging
import sys
import subprocess
from sys import exit

settings = get_project_settings()
stadtCounter = 0
db = DataBase()
conn = db.create_conn()
currentTime = None
notify = Notify()
doneIds = {}
stadtList = db.returnStadte(conn)
settings = get_project_settings()
settings['LOG_FILE'] = 'Log_' + datetime.now().strftime("%Y-%m-%d") + '.log'
settings['LOG_LEVEL'] = 'WARNING'
process = CrawlerProcess(settings)
notify = Notify()
logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)

# A callback that unpacks and prints the results of a DeferredList


def printResult(result, stadtid, timebeforeCrawl):
    print("ENDE CRALER %s  MIT STADTID %s  : " %
                    (str(stadtid), str(currentTime)))
    global stadtCounter
    notify.showBenachrichtung(stadtid, timebeforeCrawl)

    print(str(stadtid) + " WURDE NOTIFIED")
    killChromies()
    _crawl()


def killChromies():
    for proc in psutil.process_iter():
        try:
            # check whether the process name matches
            if proc.name() == 'Google Chrome' or proc.name() == 'chromedriver' or proc.name() == 'chrome' or proc.name() == 'google-chrome':
                proc.kill()
          
        except Exception as e:
            print(e)
            
def checkIfScriptIsRunning():
    cmd = ['pgrep -x python3']
    process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, 
    stderr=subprocess.PIPE)
    my_pid, err = process.communicate()
    print(str(len(my_pid.splitlines())))
    if len(my_pid.splitlines()) >1:
        print("CRAWLER ALREADY Running, STOP")
        exit(0)
   
'''def neverEndingLoopBack():
    global stadtList
    now = datetime.now()
    for id, zeit in list(doneIds.items()):
        if datetime.strptime(zeit, '%Y-%m-%d %H:%M:%S') <= now - timedelta(hours=1):
            print(str(doneIds.keys()))
            print(str(id) + " : " + str(zeit) +
                            " werden nun entfernt , da laenger als eine STunde her")
            doneIds.pop(id)
            print(str(doneIds.keys()))

    if row.get('Id') in doneIds:

        print(str(row.get('Id')) + " in liste")
        _crawl()
    else:
        currentTime = datetime.now()
        doneIds[row.get('Id')] = currentTime.strftime('%Y-%m-%d %H:%M:%S')
    db.closeAllConnections(conn)
    reactor.stop()
    print("MIT ALLEN FERTIG HORE AUF, MACHE PAUSE")'''


def oneKeyboardInterruptHandler(failure):
    failure.trap(KeyboardInterrupt)
    print("INTERRUPTING one(): KeyboardInterrupt")
    process.stop()

def crash(failure):
    print('oops, spider crashed: || ' + str(failure))
    print(failure.getTraceback())
    killChromies()
    process.stop()


def _crawl():

    global stadtList
    if stadtList is None:
        return
    if stadtList:
        row = stadtList.pop()
       # print('RUNNER: mache ' + str(row))
    else:
        print('ALLE ABGEARBEITET STOPPE NUN')
        process.stop()
        
        
    currentTime = datetime.now()

    print("STARTE CRALER:STARTPUNKT : " + str(currentTime))
    immo = process.crawl(ImmoSpider, kritId=row.get('Kritid'))
    net = process.crawl(ImmonetSpider, kritId=row.get('Kritid'))
    meinestadt = process.crawl(MeineStadtSpider, kritId=row.get('Kritid'))
    sparkasse = process.crawl(SparkasseSpider, userToStadt=row)

    dl = defer.DeferredList(
        [ immo, net], consumeErrors=True)
    dl.addCallback(printResult, stadtid=row.get(
        'Stadtid'), timebeforeCrawl=currentTime)
    dl.addErrback(oneKeyboardInterruptHandler)
    dl.addErrback(crash)  # <-- add errback here

    return dl



print("CRAWLER FANGT AN: ")
_crawl()
process.start()
