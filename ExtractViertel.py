from database import DataBase

import logging


class ExtractViertel:

    db = None

    def init(self):
        self.db = DataBase()

    def extractAdresse(self, viertel, anbieter, stadtid):

     

        # if anbieter == 0:
        #      # Wenn 3 Parts, Viertel steckt im 2 Part
        #     # WEnn 2 Parts, Viertel steckt im 1 Part
        #     viertelPart = adresse.split(",")
        #     if(len(viertelPart) > 2):
        #         viertel = viertelPart[1].replace(",", "")
        #     else:
        #         viertel = viertelPart[0].strip()

        # if anbieter == 1:
        #     if "(" in adresse:
        #         viertel = adresse[adresse.index(
        #             '(')+1: adresse.index(')')].strip()
        #     else:
        #         return "0"
            
        if  viertel is not None and len(viertel) != 0:
            stadtviertelIndex = self.db.findStadtViertel(viertel, stadtid)
            
        if stadtviertelIndex:
            return stadtviertelIndex
        else:
            return None
