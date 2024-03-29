#!/usr/bin/env python
# -*- coding: utf-8 -*-

from database import DataBase
from scrapy.utils.project import get_project_settings
import scrapy.crawler as crawler
from scrapy.crawler import CrawlerProcess
from datetime import datetime
import time
from datetime import timedelta
import logging
from scrapy.utils.log import configure_logging
import requests
import json
settings = get_project_settings()
stadtCounter = 0
db = DataBase()
# conn = db.create_conn()
currentTime = None
stadtList = db.findAllStadtUrl()
settings = get_project_settings()
settings['LOG_FILE'] = 'Log_' + datetime.now().strftime("%Y-%m-%d") + '.log'
settings['LOG_LEVEL'] = 'WARNING'
process = CrawlerProcess(settings)

immoAnbieter = [ "immonet", "immoscout", "meinestadt"]
 
nodes = [ 'http://herokuapp.com:80/schedule.json' ]


# A callback that unpacks and prints the results of a DeferredList

def printResult(result, stadtid, timebeforeCrawl):
    # print("ENDE CRALER %s  MIT STADTID %s  : " %
    #                 (str(stadtid), str(currentTime)))
    # global stadtCounter
    # notify.showBenachrichtung(stadtid, timebeforeCrawl)

    # print(str(stadtid) + " WURDE NOTIFIED")
    # killChromies()
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

    global stadtList, stadtCounter
    
    for entry in stadtList:

        node = nodes[stadtCounter]
        db.deletEentryBeforeCrawl(entry)
        print('NODE '+ node + ' MACHT ENTRY '+ str(entry['stadtname']) + str(entry['haus']) + str(entry['kaufen']) ) 
        stadtCounter += 1
        if stadtCounter > 9:
            stadtCounter = 0
            print('MACHE PAUSE')
            time.sleep(60 * 2)  
        stadtid = entry['_id'] 
    
        data = {
            'project' : 'default',
            'spider' : 'immonet',
            'setting' : 'CLOSESPIDER_PAGECOUNT=10'
                        'stadtId' : stadtid
        }

        for anbieter in immoAnbieter:
            data['spider'] = anbieter
            # print(data)
            response = requests.post(node, data=data)
            # print(response.text)
        # immo = process.crawl(ImmoSpider, stadtId=stadtid)
        # net = process.crawl(ImmonetSpider, stadtId=stadtid)
        # meinestadt = process.crawl(MeineStadtSpider,stadtId=stadtid)
        # sparkasse = process.crawl(SparkasseSpider, stadtId=stadtid)

        # dl = defer.DeferredList(
        #     [ immo, net, meinestadt], consumeErrors=True)
        # dl.addCallback(printResult, stadtid=stadtid, timebeforeCrawl=currentTime)
        # dl.addErrback(oneKeyboardInterruptHandler)
        # dl.addErrback(crash)  # <-- add errback here

        # return dl



print("STARTE CRAWLER : " + str(datetime.now()))
_crawl()
print("ENDE CRAWLER : " + str(datetime.now())  )

process.start()
