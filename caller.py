#!/usr/bin/env python
# -*- coding: utf-8 -*-

from database import DataBase
from datetime import datetime
import requests
import json
import time

db = DataBase()

stadtList = db.findAllStadtUrl()
stadtCounter = 0

# immoAnbieter = [ "immonet", "immoscout", "meinestadt", "sparkasse", "wohnungsmarkt24"]
immoAnbieter = [ "immonet", "immoscout", "meinestadt"]

nodes = [ 'http://immorobo.herokuapp.com:80/schedule.json', 'http://immorobo-1.herokuapp.com:80/schedule.json',
'http://immorobo-2.herokuapp.com:80/schedule.json','http://immorobo-3.herokuapp.com:80/schedule.json',
'http://immorobo-4.herokuapp.com:80/schedule.json' ]

nodes2 = [ 'http://immorobo-5.herokuapp.com:80/schedule.json',  'http://immorobo-6.herokuapp.com:80/schedule.json',
'http://immorobo-7.herokuapp.com:80/schedule.json',  'http://immorobo-8.herokuapp.com:80/schedule.json',
'http://immorobo-9.herokuapp.com:80/schedule.json'     ]

nodes = nodes + nodes2
# A callback that unpacks and prints the results of a DeferredList

def _crawl():

	global stadtList, stadtCounter
	for entry in stadtList:
     
		if stadtCounter > 9:
			stadtCounter = 0
			print('MACHE PAUSE')
			time.sleep(60 * 2)  
		
		node = nodes[stadtCounter]
		stadtCounter += 1
  		if datetime.now().hour == 7:
			db.deleteEntriesFromYesterday(entry)
	
		print('NODE '+ node + ' MACHT ENTRY '+ str(entry['stadtname']) + str(entry['haus']) + str(entry['kaufen']) ) 
		
		stadtid = entry['_id'] 

		data = {
		'project' : 'default',
		'spider' : 'immonet',
		'stadtId' : stadtid
		}

		for anbieter in immoAnbieter:
			data['spider'] = anbieter
			response = requests.post(node, data=data)
			#print(response.text)



print("STARTE CRAWLER : " + str(datetime.now()))
_crawl()
print("ENDE CRAWLER : " + str(datetime.now())  )

