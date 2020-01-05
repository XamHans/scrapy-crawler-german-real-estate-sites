from database import DataBase

import logging


class ExtractViertel:

    db = None

    def init(self):
        self.db = DataBase()

    def extractAdresse(self, conn, adresse, anbieter, stadtid):

        if adresse is None or stadtid is None:
            return

        viertelPart = []
        viertel = adresse

        if anbieter == 0:
             # Wenn 3 Parts, Viertel steckt im 2 Part
            # WEnn 2 Parts, Viertel steckt im 1 Part
            viertelPart = adresse.split(",")
            if(len(viertelPart) > 2):
                viertel = viertelPart[1].replace(",", "")
            else:
                viertel = viertelPart[0].strip()

        elif anbieter == 1:
            if "(" in adresse:
                viertel = adresse[adresse.index(
                    '(')+1: adresse.index(')')].strip()
            else:
                return "0"
        if  viertel and viertel is not None and len(viertel) != 0:
            sql = "Select id from StadtViertel Where UPPER(StadtViertel) Like '%s' and Stadtid = %s " % (
                str(viertel).upper().strip(), stadtid)

            stadtvid = self.db.returnStadtVidFromViertel(conn, sql)

        if stadtvid:
            return stadtvid
        else:
            return 0
