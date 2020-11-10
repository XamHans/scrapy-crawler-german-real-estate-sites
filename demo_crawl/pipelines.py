# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pymysql
from pymysql import IntegrityError
import traceback
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from geopy.geocoders import Nominatim
import geocoder
from demo_crawl.items import ImmobilieItem
import re
import datetime
from ExtractViertel import ExtractViertel
from database import DataBase
import traceback
import logging
import uuid
from telegram import Telegram
import requests
class MongoDbPipeline(object):

    stopCondition = 0
    telegramMsgCount = 0
    extractor = None
    conn = None
    mydb = None
    def getLanLonOSM(self, add, item):

        geolocator = Nominatim(user_agent="IR")
        try:

            location = geolocator.geocode(add)
            if add in str(location):
                item['lat'] = location.latitude
                item['lon'] = location.longitude
                self.ermittleStadtvidFromSuburb(item)

        except Exception as e:
            print(e)

    def getSuburbOSM(self, lat, lon):

        geolocator = Nominatim(user_agent="IR")
        try:
            cord = "%s,%s" % (lat, lon)
            location = geolocator.reverse(cord)
            if 'address' in location.raw and 'suburb' in location.raw:
                return location.raw['address']['suburb']
            return None
        except Exception as e:
            print(e)

    def validAddresse(self, add):
        add = str(add).replace('.', "")
        m = re.search("[a-zA-Z]+\s\d{1,3}", add)

        if not m:
            return False

        return True

    def ermittleStadtvidFromSuburb(self, item):
        if 'ort' in item and ',' in str(item['ort']):
            bezirk = str(item['ort']).split(',')[1]
        elif 'adresse' in item:
            bezirk = str(item['adresse'])
            if item['anbieter'] != "2" and ',' in item['adresse']:
                bezirk = str(item['adresse']).split(
                    ',')[1].replace('Ortsteil', '')
            stadtvid = self.getStadtVid(bezirk, item)
            if stadtvid == 0:
                bezirk = self.getSuburbOSM(item['lat'], item['lon'])
        elif 'stadtvid' not in item and 'lat' in item:
            bezirk = self.getSuburbOSM(item['lat'], item['lon'])
        else:
            return
     

    def getLanLonMapQuest(self, address, item):

        try:
            add = address
            if self.validAddresse(add) == False:
                return

            r = re.search("([^\s]+)\s\d{1,3}", add)
            if not 'stadtname' in item:
                return
            add = str(r.group(0)) + "," + item["stadtname"]
            g = geocoder.mapquest(add, key='jeZSRierVHGDw7Wi0cseGBzo4My34gS2')
            if g:
                if g.street:
                    item['lat'] = g.lat
                    item['lon'] = g.lng
                else:
                    self.getLanLonOSM(add, item)

        except Exception as e:
            print(e)
            print(e)

    def __init__(self):
        try:
            self.mongo_uri = 'mongodb://root:schranknr8@173.212.249.71:27017'
            self.mongo_db = 'immo_db'
            self.mydb = DataBase()
            # self.conn = self.mydb.create_conn()
            self.extractor = ExtractViertel()
            self.extractor.init()
        except Exception as e:
            print(e)

    def open_spider(self, spider):
        try:
            self.client = MongoClient(self.mongo_uri)
            self.db = self.client[self.mongo_db]
        except Exception as e:
            print(e)

    def close_spider(self, spider):
        try:
            self.client.close()
            # self.mydb.closeAllConnections(self.conn)
            self.stopCondition = 0
        except Exception as e:
            print(e)
            
    def transformItem(self, item):
        stadt = self.mydb.findStadt(item['stadtid'])
        ausstattungArray = []
        print('ITEM URL IST ' + str(item['url']))
        transObject = {
            '_id': str(uuid.uuid4()),
            'immobilienTypDaten': 
                                {
                                    'immoRentType': item['kaufen'],
                                    'immoType': item['haus']
                                },
            'standortDaten':    
                                {
                                    'Stadt': stadt,
                                },
            'basisDaten':    
                                {
                                    'flache': item['flache'],
                                    'zimmer': item['zimmer'],
                                    'bezugsfreiab': item['bezugsfreiab'] if 'bezugsfreiab' in item else None
                                },

            'beschreibungDaten': 
                                {
                                    'title': item['title']
                                },
            'fotoDaten': 
                                {
                                    'images': item['images']
                                },
            'url': item['url'],
            'anbieter': item['anbieter'],
            'createdAt':  datetime.datetime.utcnow(),

        }
        transObject['standortDaten']['Stadt']['Stadtviertel'] = []
        if 'stadtvid' in item and item['stadtvid'] is not None:
            transObject['standortDaten']['Stadt']['Stadtviertel'] = {'index': item['stadtvid']}
        if 'adresse' in item:
            transObject['standortDaten']['strasse'] = item['adresse']
            if len(str(transObject['standortDaten']['strasse'])) > 100:
                print('ERROR STRASSE ' + str(transObject['standortDaten']['strasse']))
                transObject['standortDaten']['strasse'] = ''
        if item['kaufen'] == 0:
            transObject['mietDaten'] = { 'gesamtkosten': item['gesamtkosten']}
        else:
            transObject['kaufDaten'] = { 'kaufpreis': item['gesamtkosten']}
        if 'keller' in item:
            ausstattungArray.append( {
                '_id': 1,
                'name': 'Keller'
            }) 
        if 'haustier' in item:
            ausstattungArray.append( {
                '_id': 2,
                'name': 'Haustiere erlaubt'
            }) 
        if 'ebk' in item:
            ausstattungArray.append( {
                '_id': 3,
                'name': 'Einbauküche'
            }) 
        if 'provisionsfrei' in item:
            ausstattungArray.append( {
                '_id': 9,
                'name': 'Provisionsfrei'
            }) 
        if 'garage' in item:
            ausstattungArray.append( {
                '_id': 4,
                'name': 'Garage'
            }) 
        if 'terrasse' in item:
            ausstattungArray.append( {
                '_id': 5,
                'name': 'Terrasse'
            }) 
        if 'garten' in item:
            ausstattungArray.append( {
                '_id': 10,
                'name': 'Garten'
            }) 
        if 'balkon' in item:
            ausstattungArray.append( {
                '_id': 6,
                'name': 'Balkon'
            }) 
        if 'aufzug' in item:
            ausstattungArray.append( {
                '_id': 7,
                'name': 'Aufzug'
            }) 
        if 'mobliert' in item:
            ausstattungArray.append( {
                '_id': 11,
                'name': 'Möbliert'
            }) 
        if 'barriefrei' in item:
            ausstattungArray.append( {
                '_id': 8,
                'name': 'Barrierefrei'
            }) 
        if len(ausstattungArray) > 0:
            transObject['ausstattungDaten'] = ausstattungArray
        return transObject
    
    def transformWGItem(self, item):
        stadt = self.mydb.findStadt(item['stadtid'])
        ausstattungArray = []
        print('WGITEM URL IST ' + str(item))
        transObject = {
            '_id': str(uuid.uuid4()),
            'immobilienTypDaten': 
                                {
                                    'immoType': item['haus'],
                                    'immoRentType': 0
                                },
            'standortDaten':    
                                {
                                    'Stadt': stadt
                                },
            'basisDaten':    
                                {
                                    'zimmerflache': item['zimmerflache']
                                },
            'mietDaten':    
                                {
                                    'gesamtkosten': item['gesamtkosten']
                                },

            'beschreibungDaten': 
                                {
                                    'title': item['title']
                                },
            'fotoDaten': 
                                {
                                    'images': item['images']
                                },
            'url': item['url'],
            'anbieter': item['anbieter'],
            'createdAt':  datetime.datetime.utcnow(),
        }


        if transObject["anbieter"] == "6":
            if int(transObject["basisDaten"]["zimmerflache"]) > 35:
                transObject["basisDaten"]["flache"] = transObject["basisDaten"]["zimmerflache"]
                del transObject["basisDaten"]["zimmerflache"]
        
        if 'gesamtflache' in item:
            transObject["basisDaten"]["flache"] =  item['gesamtflache']

      
        if 'adresse' in item:
            transObject["standortDaten"]["strasse"] =  item['adresse']

        if 'bezugsfreiab' in item:
            transObject["basisDaten"]["bezugsfreiab"] =  item['bezugsfreiab'] 
            
        transObject["wgDaten"] = {}
        if 'anzahlf' in item:
            transObject["wgDaten"]["anzahlf"] =  item['anzahlf'] 
        if 'anzahlm' in item:
            transObject["wgDaten"]["anzahlm"] =  item['anzahlm'] 
        if 'gesuchtf' in item:
            transObject["wgDaten"]["gesuchtf"] =  item['gesuchtf'] 
        if 'gesuchtm' in item:
            transObject["wgDaten"]["gesuchtm"] =  item['gesuchtm'] 

        if 'keller' in item:
            ausstattungArray.append( {
                '_id': 1,
                'name': 'Keller'
            }) 
        if 'haustier' in item:
            ausstattungArray.append( {
                '_id': 2,
                'name': 'Haustiere erlaubt'
            }) 
        if 'ebk' in item:
            ausstattungArray.append( {
                '_id': 3,
                'name': 'Einbauküche'
            }) 
        if 'provisionsfrei' in item:
            ausstattungArray.append( {
                '_id': 9,
                'name': 'Provisionsfrei'
            }) 
        if 'garage' in item:
            ausstattungArray.append( {
                '_id': 4,
                'name': 'Garage'
            }) 
        if 'terrasse' in item:
            ausstattungArray.append( {
                '_id': 5,
                'name': 'Terrasse'
            }) 
        if 'garten' in item:
            ausstattungArray.append( {
                '_id': 10,
                'name': 'Garten'
            }) 
        if 'balkon' in item:
            ausstattungArray.append( {
                '_id': 6,
                'name': 'Balkon'
            }) 
        if 'aufzug' in item:
            ausstattungArray.append( {
                '_id': 7,
                'name': 'Aufzug'
            }) 
        if 'mobliert' in item:
            ausstattungArray.append( {
                '_id': 11,
                'name': 'Möbliert'
            }) 
        if 'barriefrei' in item:
            ausstattungArray.append( {
                '_id': 8,
                'name': 'Barrierefrei'
            }) 
        if len(ausstattungArray) > 0:
            transObject['ausstattungDaten'] = ausstattungArray
        return transObject

    def process_item(self, item, spider):
        try:
            print('PROCESS ITEM MIT URL ' + str(item['url']))
            if self.stopCondition >= 35:
                print('STOPPE PROCESS ZU VIELE DUPLICATE ERORS : ' +
                    str(self.stopCondition))
                self.stopCondition = 0
                print(
                    "ZU VIELE DUPLICATE ERROR; STOPPE SPIDER FROM PIPELINE")
                spider.stop = True
          
            if 'url' in item and 'title' in item:
                item['createdat'] = datetime.datetime.now().strftime(
                    '%Y-%m-%d %H:%M:%S')

                if 'adresse' not in item:
                    if 'ort' in item:
                        item['adresse'] = item['ort']

                try:
                    if 'stadtname' in item:
                        del item['stadtname']
                    if item["haus"] == 2:
                        mongoStructureItem = self.transformWGItem(item)
                    else:
                        mongoStructureItem = self.transformItem(item)

                    self.mydb.insertMongoImmos(mongoStructureItem)
                    if item['images'] and 'gesamtkosten' in item:
                        if int(item['gesamtkosten']) < 2000:
                            Telegram.send_message(item)
                            
                except Exception as e:
                    print('FEHLER' + str(e))
                    logging.warning(e)
                    # self.db[self.collection].update_one({"url": item['url']}, { "$set": {"alive": datetime.datetime.now().strftime(
                    # '%Y-%m-%d %H:%M:%S')} })
                    self.stopCondition += 1
            else:
                print('KEIN TITLE ODER URL')
        except Exception as e:
            print('FEHLER PIPELINE:')
            traceback.print_exception(type(e), e, e.__traceback__)

        return item


