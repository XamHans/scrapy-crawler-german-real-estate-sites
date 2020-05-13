#!/usr/bin/env python
# -*- coding: utf-8 -*-

from database import DataBase
from datetime import datetime
import requests
import json
import time

db = DataBase()

stadtList = db.findAllWGStadtUrl()
stadtCounter = 0

immoAnbieter = [ "wgsuche", "ebay"]

nodes = [ 'http://immorobo.herokuapp.com:80/schedule.json', 'http://immorobo-1.herokuapp.com:80/schedule.json',
'http://immorobo-2.herokuapp.com:80/schedule.json','http://immorobo-3.herokuapp.com:80/schedule.json',
'http://immorobo-4.herokuapp.com:80/schedule.json' ]


# A callback that unpacks and prints the results of a DeferredList

def _crawl():

	global stadtList, stadtCounter
	for entry in stadtList:
     
		if stadtCounter > 1:
			stadtCounter = 0
			print('MACHE PAUSE')
			time.sleep(60 * 2)  
		
		node = nodes[stadtCounter]
		stadtCounter += 1

		db.deleteEntriesFromYesterday(entry)
	
		print('NODE '+ node + ' MACHT ENTRY '+ str(entry['stadtname']) + str(entry['haus']) + str(entry['kaufen']) ) 
		
		stadtid = entry['_id'] 

		data = {
		'project' : 'default',
		'spider' : 'immonet',
		'setting' : 'CLOSESPIDER_PAGECOUNT=10',
		'setting' : 'CLOSESPIDER_TIMEOUT=60',
		'stadtId' : stadtid
		}

		for anbieter in immoAnbieter:
			data['spider'] = anbieter
			response = requests.post(node, data=data)
			print(response.text)



print("STARTE CRAWLER : " + str(datetime.now()))
_crawl()
print("ENDE CRAWLER : " + str(datetime.now())  )

