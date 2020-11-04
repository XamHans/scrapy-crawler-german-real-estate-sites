from database import DataBase
from pyfcm import FCMNotification
import time
import traceback
import logging
import requests
import pymongo
import threading
from datetime import datetime, timedelta

class Notify:

    entry = None
   
    anfang = None

    def __init__(self,entry):
        self.db = DataBase()
        self.entry = entry
        self.anfang = datetime.now()
        self.showBenachrichtung(entry)
        #threading.Timer(5.0, self.showBenachrichtung).start()
    
    def immosWithPictures(self, immo):
        if immo['fotoDaten']['images']:
            return True
        return False
    
    def showBenachrichtung(self, entry):
        try:
            route = 'https://www.immorobo.de/list/' + str(entry['stadtname'])  + '/';
            if (int(entry['haus']) == 0):
                route += 'wohnung'
            elif (int(entry['haus']) == 1):
                route += 'haus'
            else:
                route += 'wg';        

            if (int(entry['kaufen']) == 0):
                route += '/mieten';
            else:
                route += '/kaufen';
            foundImmos = self.db.checkIfNewImmos(self.anfang, datetime.now(), self.entry)
            foundImmos = list(filter(self.immosWithPictures, foundImmos))

            if foundImmos:
                notify = {
                    'stadt':        entry['stadtname'],
                    'immoType':     int(entry['haus']),
                    'immoRentType': int(entry['kaufen']),
                    'image':        foundImmos[0]['fotoDaten']['images'][0],
                    'url': route    
                    }
                response = requests.post("http://localhost:3000/api/notifications/notify", data=notify)
            # ende =  - timedelta(hours=1)
            #ende = datetime.now() + timedelta(minutes=5)
            #print('anfang', str(self.anfang), '  ende ', str(ende))
            # print("ZEIGE BENACHRichtung für STADT: " + str(self.entry['stadtid']) + " von " + self.anfang + " - " + ende)
            # foundImmos = self.db.checkIfNewImmos(self.anfang, ende, self.entry)
            #if len(foundImmos) > 0:
              #  print('foundImmos ist größer 0 , notify requests geht raus')
        except Exception as ex:
            traceback.print_exc()

    def showNeuKunden(self, anfangsZeitpunkt):

        anfang = anfangsZeitpunkt
        ende = time.strftime('%Y-%m-%d %H:%M:59')
        print("ZEIGE BENACHRichtung von " + anfang + " - " + ende)

        sql = "SELECT COUNT(*) as Count FROM robo.Kriterien Where created_at between '%s' AND '%s' " % (anfang, ende)
        print(sql)
        count = self.db.returnTrefferVonU
        serInZeitraum(self.conn, sql)
        if int(count.get("Count")) > 0:
            print("JUHU NEUE KUNDEN")
            registration_id = "dVWKUjvNogk:APA91bGAUHHSPaT8N63Iq4nRNl3RzqZ0xDWoSe4kv6Yp1a9bH1_jSbn1Tjjd4wvmwiVwrSLbZ4ZrrJYskrruEeOtbetsH7Wgg-EpgrAV8-eAvGPbHFiWqx651p9zDLrOtpkydGfeRN_S"
            message_title = "ImmoRobo"
            message_body = "Hi, es gibt "
            str(count.get("Count")) + " neue Kunden"
            result = self.push_service.notify_single_device(
                registration_id=registration_id, message_title=message_title, message_body=message_body)
            print(result)

