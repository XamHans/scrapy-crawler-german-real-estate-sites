from database import DataBase
from pyfcm import FCMNotification
import time
import traceback
import logging
import requests
import pymongo


class Notify:

    db = None
    myclient = None
    mongoDb = None
    immosCol = None
    countQuery = None

    push_service = FCMNotification(
        api_key="AAAAy9N4W0c:APA91bH62Vv_zkFSMLBjYIQ3fePeFRnixCErDySDHu8tmAIy8afBD-7c4CTWCwkiE82UOYlRTsCTKQ-Pf4Y1ItXVTPMflPMIn0Fenm-NgISP_eErZTiZ_HYp_nZUf2CB-D9WF2x5tgh_")

    def __init__(self):
        self.db = DataBase()
        self.myclient = pymongo.MongoClient(
            "mongodb://root:schranknr8@173.212.249.71:27017/")
        self.mongoDb = self.myclient["immo_db"]
        self.immosCol = self.mongoDb["immos"]

    def showBenachrichtung(self, stadtid, anfangsZeitpunkt):
        try:

            anfang = anfangsZeitpunkt.strftime('%Y-%m-%d %H:%M:00')
            ende = time.strftime('%Y-%m-%d %H:%M:59')
            print("ZEIGE BENACHRichtung von " + anfang + " - " + ende)
            conn = self.db.create_conn()
            userVonStadt = self.db.returnUserFromStadt(conn, stadtid)
            if not userVonStadt:
                return
            for user in userVonStadt:

                sql = "SELECT COUNT(*) as Count FROM robo.WohnungsView Where Datum between '%s' AND '%s'  AND Stadtid = %s AND  Gesamtmiete <= %s AND Zimmer >= %s AND Wohnflache >= %s  AND Kaufen = %s" \
                    % (anfang, ende, stadtid, user.get("KaltmieteBis"), user.get("ZimmerAb"), user.get("Wohnflache"), user.get("Kaufen"))
                kaltmiete = user.get("KaltmieteBis")
                if  kaltmiete:
                    self.countQuery = {"createdat": {"$gte": anfang, "$lte": ende},
                        "gesamtkosten": {"$lte": float(user.get("KaltmieteBis"))},
                        "zimmer": {"$gte": float(user.get("ZimmerAb"))},
                        "flache": {"$gte": float(user.get("Wohnflache"))},
                        }
                else:
                    self.countQuery = {"createdat": {"$gte": anfang, "$lte": ende},
                        "zimmer": {"$gte": float(user.get("ZimmerAb"))},
                        "flache": {"$gte": float(user.get("Wohnflache"))},
                        }
                #print('notify anfang '+ str(anfang) + ' -  '+ str(ende))
                if user.get('Haus') is None:
                    sql = sql + ' AND Haus IS NULL'
                    # check how search mongodb where field IS NOT
                else:
                    sql = sql + ' AND Haus = ' + str(user.get('Haus'))
                    self.countQuery['haus'] = user.get('Haus')
                if user.get('StadtVid'):
                    sql = sql + ' AND StadtVid =' + str(user.get('StadtVid'))
                    self.countQuery['stadtvid'] = user.get('StadtVid')
                if user.get('EBK'):
                    sql = sql + ' AND EBK = 1'
                    # here so earch where field ebk exists, no need to search for 1
                    self.countQuery['ebk'] = 1
                if user.get('Haustiere'):
                    sql = sql + ' AND Haustiere = 1'
                    self.countQuery['haustiere'] = 1
                if user.get('Garage'):
                    sql = sql + ' AND Garage = 1'
                    self.countQuery['garage'] = 1
                if user.get('Keller'):
                    sql = sql + ' AND Keller = 1'
                    self.countQuery['keller'] = 1
                if user.get('Balkon'):
                    sql = sql + ' AND Balkon = 1'
                    self.countQuery['balkon'] = 1
                if user.get('Terasse'):
                    sql = sql + ' AND Terasse = 1'
                    self.countQuery['terasse'] = 1
                if user.get('Barriefrei'):
                    sql = sql + ' AND Barriefrei = 1'
                    self.countQuery['barriefrei'] = 1
                if user.get('Garten'):
                    sql = sql + ' AND Garten = 1'
                    self.countQuery['garten'] = 1
                if user.get('Aufzug'):
                    sql = sql + ' AND Aufzug = 1'
                    self.countQuery['aufzug'] = 1
                if user.get('Provisionsfrei'):
                    sql = sql + ' AND Provisionsfrei = 1'
                    self.countQuery['provisionsfrei'] = 1

      
                count = self.immosCol.find(self.countQuery).count()
                if int(count) > 0:

                    registration_id = user.get("Notifytoken")
                    print('notify to: '+str(registration_id))
                    message_title = "ImmoRobo"
                    message_body = "Hi, es gibt " \
                        + str(count) + " neue Immobilien"
                    result = self.push_service.notify_single_device(registration_id=registration_id, message_title=message_title, message_body=message_body)
                    #print(result)
            self.db.closeAllConnections(conn)


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

