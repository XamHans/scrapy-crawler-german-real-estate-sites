#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time
import traceback
import logging
from pymongo import MongoClient
import pymongo
from bson.objectid import ObjectId
from datetime import date, timedelta, datetime

class DataBase:

 
    mongo_immos = None
    myclient = pymongo.MongoClient("mongodb://173.212.249.71:30001")
    mydb = myclient["immo_db"]
    mongo_immos = mydb["immos"] 
  

   
    def insertUrlsForKrit(self, kritUrls):
        try:
             self.mydb['stadturls'].insert_one(dict(kritUrls))
        except Exception as e:
            print(e)
            
    def deleteEntriesFromYesterday(self, entry):

        yesterday = datetime.today() - timedelta(days=2)

        self.mydb['immos'].delete_many({  'anbieter': { "$exists": True},
                                         'createdAt' : {"$lt" :  yesterday },
                                        'standortDaten.Stadt.id' : entry['stadtid'],
                                        'immobilienTypDaten.immoRentType': entry['kaufen'],
                                        'immobilienTypDaten.immoType': entry['haus'] })
        
    def findStadtUrls(self, stadtid):
        foundStadtUrls = self.mydb['stadturls'].find_one({'_id': ObjectId(stadtid)})
        return foundStadtUrls
    
    def findAllStadtUrl(self):
        foundStadtUrls = self.mydb['stadturls'].find()
        return foundStadtUrls
    
    def findStadt(self, stadtid):
        foundStadt = self.mydb['stadte'].find_one({'id': int(stadtid)})
        return foundStadt
    
    def findStadtViertel(self, viertel, stadtid):
        print('VIERTEL IST ' + str(viertel))
        cursor = self.mydb['stadte'].aggregate([
           { "$match" : {"id": stadtid }},
           { "$project": {  "index":
                                 { 
                                  "$indexOfArray": [ "$Stadtviertel", viertel ] 
                                 }
                         }
            }
        ])
        result = list(cursor)

        print('STADTVIERTELINDEX IST ' + str(result))
        if result and result[0]['index'] >= 0:
            print(result[0]['index'])
            return result[0]['index']
        else:
            return None

    def checkIfInDupUrl(self, url):
        try:
                # Create a new record
                if "?" in str(url):
                    url = url.split('?')[0]
                if self.mongo_immos.count_documents({ 'url': url }, limit = 1) != 0: 
                    return True
                return False
        except Exception as e:
            print(e)
            return False
        
    def insertMongoImmos(self, jsonObject):
        try:
             self.mongo_immos.insert_one(jsonObject)
        except Exception as e:
            print(e)
            return False

    def returnUrlsFromStadt(self, stadtid):

        returner = self.mongo_immos.find({'stadtid': stadtid}, {'url'}, limit=50).sort([("createdat", pymongo.ASCENDING)])
        return list(self.mongo_immos.find({}, {'url'}))

  
            
    def returnChangedKritids(self):
        toDoStadte = []
      
        toDoStadte.append( {'Haus':0, 'Kaufen': 0, 'Stadtid': 461, 'Stadt': 'Amberg' })
        toDoStadte.append( {'Haus':0, 'Kaufen': 1, 'Stadtid': 461, 'Stadt': 'Amberg' })
        toDoStadte.append( {'Haus':1, 'Kaufen': 1, 'Stadtid': 461, 'Stadt': 'Amberg' })
        return toDoStadte
            
  


    def deleteUrl(self, url):        
        try:
            self.mongo_immos.delete_one({'url': url})
        except Exception as e:
            print(str(e))
