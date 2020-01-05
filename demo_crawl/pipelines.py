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


class MongoDbPipeline(object):
    collection = 'immos'

    stopCondition = 0
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
        if bezirk:
            viertelrow = self.getStadtVid(bezirk, item)
            if viertelrow != 0:
                item['stadtvid'] = viertelrow

    def getStadtVid(self, bezirk, item):
        if not bezirk:
            return 0
        sql = "Select id from StadtViertel Where UPPER(StadtViertel) Like '%s' and Stadtid = %s " % (
            str(bezirk).upper().strip(), item['stadtid'])
        stadtvid = self.mydb.returnStadtVidFromViertel(
            self.conn, sql)
        return stadtvid

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
            self.conn = self.mydb.create_conn()
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
            self.mydb.closeAllConnections(self.conn)
            self.stopCondition = 0
        except Exception as e:
            print(e)

    def process_item(self, item, spider):
        try:
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

                if 'adresse' in item and 'lat' not in item:

                    addresseMitStadt = item['adresse']
                    if 'stadtname' in item:
                        addresseMitStadt = item['adresse'] + \
                            ', ' + item['stadtname']

                    self.getLanLonMapQuest(addresseMitStadt, item)
                if 'lat' in item:
                    try:
                        self.ermittleStadtvidFromSuburb(item)
                    except Exception as e:
                        print(e)
                try:
                    if 'stadtname' in item:
                        del item['stadtname']
   
                    self.db[self.collection].insert_one(dict(item))
                    #logging.warning('insert item :' +str(item))
                except Exception as e:
                    #logging.warning(e)
                    self.db[self.collection].update_one({"url": item['url']}, { "$set": {"alive": datetime.datetime.now().strftime(
                    '%Y-%m-%d %H:%M:%S')} })
                    self.stopCondition += 1

        except Exception as e:
            print('FEHLER PIPELINE:')
            traceback.print_exception(type(e), e, e.__traceback__)

        return item


class DemoCrawlPipeline(object):

    host = '173.212.249.71'
    user = 'hans'
    password = 'schranknr8'
    db = 'robo'
    stopCondition = 0

    def __init__(self):

        self.connection = pymysql.connect(
            self.host, self.user, self.password, self.db)
        self.cursor = self.connection.cursor()

    def getLanLonOSM(self, add, item):

        geolocator = Nominatim(user_agent="IR")
        try:

            location = geolocator.geocode(add)
            if add in str(location):
                item['lat'] = location.latitude
                item['lon'] = location.longitude

        except Exception as e:
            print(e)

    def validAddresse(self, add):
        add = str(add).replace('.', "")
        m = re.search("[a-zA-Z]+\s\d{1,3}", add)

        if not m:
            return False

        return True

    def getLanLonMapQuest(self, address, item):

        try:
            add = address
            if item['ort'] != 'NULL':
                add = address + item['ort']

            if self.validAddresse(add) is False:
                return

            r = re.search("([^\s]+)\s\d{1,3}", add)
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

    def process_item(self, item, spider):
        if self.stopCondition >= 35:
            self.stopCondition = 0
            print(
                "ZU VIELE DUPLICATE ERROR; STOPPE SPIDER FROM PIPELINE")
            spider.stop = True

        for field in item.fields:
            item.setdefault(field, 'NULL')

        if type(item) is ImmobilieItem:

            try:
                if item['typ'].upper() == 'ETAGENWOHNUNG':
                    item['typ'] = '5'
                elif item['typ'].upper() == 'DACHGESCHOSS':
                    item['typ'] = '9'
                elif item['typ'].upper() == 'Souterrain':
                    item['typ'] = '2'
                elif item['typ'].upper() == 'ERDGESCHOSS':
                    item['typ'] = '3'
                elif item['typ'].upper() == 'HOCHPARTERRE':
                    item['typ'] = '4'
                elif item['typ'].upper() == 'LOFT':
                    item['typ'] = '6'
                elif item['typ'].upper() == 'MAISONETTE':
                    item['typ'] = '7'
                elif item['typ'].upper() == 'PENTHOUSE':
                    item['typ'] = '8'
                elif item['typ'].upper() == 'LOFT':
                    item['typ'] = '6'
                elif item['typ'].upper() == 'NEUBAU':
                    item['typ'] = '10'
                else:
                    item['typ'] = 'NULL'

            except Exception as e:
                print(e)

            if item['adresse'] == 'NULL':
                item['adresse'] = item['ort']

            if item['adresse'] == 'NULL':
                item['adresse'] = ""

        try:
            if (item['url'] != 'NULL') and (item['title'] != 'NULL'):
                try:
                    sql = "INSERT INTO `WohnungsPool` (`Stadtid`, `StadtVid`, `Title`,`Anbieter`,`Url`) VALUES (%s,%s, %s,%s,%s)"
                    self.cursor.execute(sql, (item['stadtid'], item['stadtvid'], str(
                        item['title']).encode('utf-8'), item['anbieter'], item['url']))
                except Exception as e:
                    print(e)
                    self.stopCondition += 1
                       

                lastid = self.cursor.lastrowid

                if item['lat'] == 'NULL' and item["anbieter"] != "3":
                    self.getLanLonMapQuest(item['adresse'], item)

                if type(item) is ImmobilieItem:
                    sql = "INSERT INTO `WohnungsInfo` (`id`, `Kaltmiete`,`Zimmer`,`Wohnflache`, `BezugsfreiAb`,`Adresse`,`EBK`,`Haustiere`, `Garage`,`Keller`, \
                         `Terasse`,`Garten`,`WohnungsTypId`,`Nebenkosten`,`Gesamtmiete`,`Provisionsfrei`,`Grundstueck`,`Kaufen`,`Haus`,`Aufzug`,`Barriefrei`,`Balkon`,`Lat`,`Lon`) VALUES (%s, %s,%s,%s,%s, %s,%s,%s,%s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s,%s)"

                    self.cursor.execute(sql, (str(lastid), str(item['kaltmiete']), item['zimmer'], item['flache'], item['bezugsfreiab'], item['adresse'], str(item['ebk']),
                                              str(item['haustier']), str(item['garage']), str(
                                                  item['keller']), str(item['terrasse']),
                                              item['garten'], item['typ'], item['nebenkosten'], item['gesamtkosten'], item['provisionsfrei'],
                                              item['grundstuck'], item['kaufen'], item['haus'], item['aufzug'], item['barriefrei'], item['balkon'], item['lat'], item['lon']))
                else:
                    sql = "INSERT INTO `WGInfo` (`id`,`Gesamtmiete`,`ZimmerGroeße`, `Kaution`,`AnzahlM`,`AnzahlF`,`LookForMale`, `LookForFeMale`,`Adresse`, \
                         `Lat`,`Lon`,`BezugsfreiAb`,`Möbliert`,`Balkon`,`Barrierfrei`,`Garten`,`Aufzug`) VALUES (%s,%s,%s,%s, %s,%s,%s,%s, %s,%s, %s, %s, %s, %s, %s, %s, %s)"

                    self.cursor.execute(sql, (str(lastid), item['gesamtkosten'], item['flache'], item['kaution'], item['anzahlm'], item['anzahlf'],
                                              item['gesuchtm'], item['gesuchtf'], str(item['adresse']).encode(
                                                  'utf-8'), item['lat'], item['lon'], item['bezugsfreiab'],
                                              item['moebliert'], item['balkon'], item['barriefrei'], item['garten'], item['aufzug']))

                self.connection.commit()
                sql = "INSERT INTO `WohnungsBilder` (`id`, `Bild1`) VALUES (%s, %s)"
                self.cursor.execute(sql, (str(lastid), str(item['bild1'])))

                for i in range(2, 8):

                    bil = 'bild%s' % (i)
                    if item[bil] != 'NULL':
                        sql = "UPDATE WohnungsBilder SET  Bild%s = %s  WHERE id = %s"
                        self.cursor.execute(sql, (i, item[bil], lastid))

        except Exception as e:
            print("MYSQL ERROR:")
            print(traceback.format_exc())
            print(e)
        return item

    def close_spider(self, spider):
        self.cursor.close()
        self.connection.close()
