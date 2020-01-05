#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pymysql
import time
import traceback
import logging
from pymongo import MongoClient
import pymongo

class DataBase:

    host = '173.212.249.71'
    user = 'root'
    password = 'schranknr8'
    db = 'robo'
    connection = None
    mongo_immos = None
    myclient = pymongo.MongoClient("mongodb://root:schranknr8@173.212.249.71:27017")
    mydb = myclient["immo_db"]
    mongo_immos = mydb["immos"] 
    
    def create_conn(self):
        return pymysql.connect(host=self.host,
                               user=self.user,
                               password=self.password,
                               db=self.db,
                               use_unicode=True,
                               charset='utf8',
                               autocommit=True,
                               cursorclass=pymysql.cursors.DictCursor)

    def closeAllConnections(self, conn):

        if conn.open:
            conn.close()

    def insertUrlsForKrit(self, kritUrls):
        if self.mydb["kriturls"].count_documents({ 'kritid': kritUrls['kritid'] }, limit = 1) == 0: 
            print('bisher kein eintrag adde kritUrls')
            self.mydb['kriturls'].insert_one(dict(kritUrls))
        
        
    def findUrlsInKritCollection(self, kritid):

        foundKritUrls = self.mydb['kriturls'].find_one({'kritid': int(kritid)})
        # print(('found for kritid {} folgenden eintrag  {}').format(str(kritid), str(foundKritUrls)))
        return foundKritUrls

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

    def returnUrlsFromStadt(self, stadtid):

        returner = self.mongo_immos.find({'stadtid': stadtid}, {'url'}, limit=50).sort([("createdat", pymongo.ASCENDING)])
        return list(self.mongo_immos.find({}, {'url'}))

    def returnStadte(self, conn):
        try:
            with conn.cursor() as cursor:
                # Create a new record

                # sql = "SELECT id FROM Kriterien WHERE Changed = 0 order by CreatedAt asc"
                sql = "SELECT  Kritid, Stadtid, ANY_VALUE(Stadt) as Stadt, ANY_VALUE(StadtViertel) as StadtViertel, MAX(CreatedAt) as CreatedAt, Haus,Kaufen FROM robo.KritView WHERE Changed = 0 group by  Stadtid, Haus, Kaufen order by CreatedAt asc"
                cursor.execute(sql)
                result = cursor.fetchall()
                cursor.close()
                return result
        except Exception as e:
            print(e)
            cursor.close()
            return None

    def writeScrapStatistik(self, conn, anbieter, scrapCount):
        try:
            with conn.cursor() as cursor:

                if not scrapCount or scrapCount == 0:
                    return
                sql = "INSERT INTO `ScrapStatistik` (`Anbieter`, `ScrapCount`) VALUES (%s,%s)"
                cursor.execute(sql, (anbieter, scrapCount))
                cursor.close()
                conn.commit()
        except Exception as e:
            print(e)

    def setScrapedTime(self, conn, id):
        try:
            with conn.cursor() as cursor:
                sql = "UPDATE UserToStadt Set  Scraped = %s  Where id = %s"
                cursor.execute(sql, (time.strftime('%Y-%m-%d %H:%M:%S'), id))
                cursor.close()
        except Exception as e:
            print(e)
            
    def setChangedToKrit(self, conn, id):
        try:
            with conn.cursor() as cursor:
                sql = "UPDATE Kriterien Set  Changed = 0  Where id = %s"
                cursor.execute(sql, id)
                cursor.close()
        except Exception as e:
            print(e)

    def returnUserFromStadt(self, conn, stadtid):
        try:
            with conn.cursor() as cursor:
                # Create a new record
                sql = ("SELECT * FROM robo.UserToStadt as us, robo.Kriterien as k where Stadtid = %s and k.id = us.Kritid and k.Notifytoken IS NOT NULL  group by k.id;") % (stadtid)
                cursor.execute(sql)
                result = cursor.fetchall()
                cursor.close()
                return result
        except Exception as ex:
            traceback.print_exception(type(ex), ex, ex.__traceback__)
            cursor.close()

    def returnTrefferVonUserInZeitraum(self, conn, sql):
        try:
            with conn.cursor() as cursor:
                # Create a new record

                cursor.execute(sql)
                result = cursor.fetchone()
                cursor.close()
        except Exception:
            cursor.close()
        return result
        

    def returnStadtVidFromViertel(self, conn, sql):
        try:
            with conn.cursor() as cursor:
                # Create a new record
                cursor.execute(sql)
                result = cursor.fetchone()
                cursor.close()
                if result:
                    return result.get('id')
                else:
                    return 0
                '''if result == None:      Wenn du neue anlegen möchtest :)
                    print("StadtViertel existiert nicht, lege neues an")
                    sql = "INSERT INTO StadtViertel (StadtViertel, Stadtid) VALUES ('%s' , %s) " % (
                        Stadtviertel, stadtid)
                    cursor.execute(sql)
                    conn.commit()
                    returnStadtIdFromViertel(Stadtviertel, stadtid)
                    cursor.close()'''
        except Exception as e:
            logging.warning(
                'Fehler in returnStadtVidFromViertel in sql ' + str(sql))
            cursor.close()
            return 0

    def deleteUrl(self, url):        
        try:
            self.mongo_immos.delete_one({'url': url})
        except Exception as e:
            print(str(e))
