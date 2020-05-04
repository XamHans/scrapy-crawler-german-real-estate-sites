import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from scrapy.http.request import Request
from demo_crawl.items import ImmobilieItem
from scrapy.loader import ItemLoader
from scrapy_splash import SplashRequest
import re
import json
from scrapy.exceptions import DropItem
from database import DataBase
import requests
from scrapy import signals
import logging
from ExtractViertel import ExtractViertel
import traceback

class SparkasseSpider(scrapy.Spider):


    db = None
    name = 'sparkasse'
    stadtid = 0
    x = 0
    conn = None
    stadtname = ""
    userToStadt = None
    extractor = None

    def __init__(self, stadtId, *args, **kwargs):
        self.db = DataBase()
        # self.conn = self.db.create_conn()
        self.userToStadt = self.db.findStadtUrls(stadtId)
        if not self.userToStadt:
            print('USERTOSTADT IST NULL')
            return
        else:
            print('SPARKSSE MACHT ', self.userToStadt)
        self.extractor = ExtractViertel()
        self.extractor.init()

        super(SparkasseSpider, self).__init__(*args, **kwargs)

    def start_requests(self):

        self.Kaufen = self.userToStadt.get("kaufen")
        self.Haus = self.userToStadt.get("haus")
        self.stadtid = self.userToStadt.get("stadtid")
        self.stadtname = self.userToStadt.get("stadtname")

        if self.userToStadt.get('kaufen') == 0:
            return

        #url = "https://immobilien.sparkasse.de/api/estate/?filterOptions=%7B%22page%22:1,%22zip_city_estate_id%22:%22" + self.stadtname + \
        #    "%22,%22marketing_usage_object_type%22:%22buy_residential_house%22,%22perimeter%22:%2225%22,%22sort_by%22:%22distance_asc%22,%22limit%22:%22109%22,%22return_data%22:%22overview%22%7D&limit=99"
        yield scrapy.Request(self.userToStadt['sparkasse'], callback=self.parse)

    def parse(self, response):
        jsonresponse = json.loads(response.body_as_unicode())
        for jsonitem in jsonresponse["_embedded"]["estate"]:
            self.x = self.x + 1

            item = ImmobilieItem()
            loader = ItemLoader(item, selector=response, response=response)

            loader.add_value("title", jsonitem["freitexte"]["objekttitel"])
            loader.add_value("gesamtkosten", jsonitem["preise"]["kaufpreis"])
            loader.add_value("flache", jsonitem["flaechen"]["wohnflaeche"])

            if "anzahl_zimmer" in jsonitem["flaechen"]:
                zimmerAnzahl = jsonitem["flaechen"]["anzahl_zimmer"].split('.')[0]
                loader.add_value("zimmer",zimmerAnzahl )
           
            
            if "aussen_courtage" in jsonitem["preise"]:

                if jsonitem["preise"]["aussen_courtage"] == 'prov.frei':
                    loader.add_value("provisionsfrei", "1")
                else:
                    loader.add_value("provisionsfrei", "0")

            if "anzahl_terrassen" in jsonitem:
                loader.add_value("terrasse", "1")

            if "unterkellert" in jsonitem["ausstattung"]:
                loader.add_value("keller", "1")

            if "grundstuecksflaeche" in jsonitem["flaechen"]:
                loader.add_value(
                    "grundstuck", jsonitem["flaechen"]["grundstuecksflaeche"])
            if "geo" in jsonitem:
                item['adresse'] = jsonitem["geo"]["ort"]
                if "strasse" in jsonitem["geo"]:
                    item['adresse'] =  item['adresse'] + ', ' + jsonitem["geo"]["strasse"]
            else:
                item['adresse'] = ''
            # try:
            #     self.getViertelIDFromLatLon(
            #         jsonitem["sip"]["location"]["lat"], jsonitem["sip"]["location"]["lon"], item)
            # except Exception as e:
            #     traceback.print_exception(type(e), e, e.__traceback__)

            url = "https://immobilien.sparkasse.de/" + jsonitem["id"]

            if "display_data" in jsonitem["sip"]:

                if "Garten" in jsonitem["sip"]["display_data"]["specials"]:
                    loader.add_value("garten", "1")

                if "Balkon" in jsonitem["sip"]["display_data"]["specials"]:
                    loader.add_value("balkon", "1")

                if "Personenaufzug" in jsonitem["sip"]["display_data"]["specials"]:
                    loader.add_value("aufzug", "1")

                if "Stellplatz" in jsonitem["sip"]["display_data"]["specials"] or "Garage" in jsonitem["sip"]["display_data"]["specials"]:
                    loader.add_value("garage", "1")

                if "Terrasse" in jsonitem["sip"]["display_data"]["specials"]:
                    loader.add_value("terrasse", "1")

            if "EBK" in jsonitem:
                val = jsonitem["kueche"]["attributes"]["EBK"]
                if val == "true":
                    loader.add_value("ebk", "1")

            if "gartennutzung" in jsonitem:
                val = jsonitem["ausstattung"]["gartennutzung"]
                if val == "true":
                    loader.add_value("garten", "1")

            loader.add_value('url', url)
            loader.add_value('stadtid', self.stadtid)
            loader.add_value('anbieter', "3")
            loader.add_value('kaufen', self.Kaufen)
            loader.add_value('haus', self.Haus)

            for i in range(1, 8):
                try:
                    bil = 'bild%s' % str(i)
                    value = jsonitem["sip"]["images"][i]["formats"]["m"]
                    loader.add_value(bil, value)
                except Exception:
                    pass
                
            images = []
            for i in jsonitem["sip"]["images"]:
                try:
                    if not i:
                        break
                    images.append(i["formats"]["original"])
                except Exception as e:
                    traceback.print_exception(type(e), e, e.__traceback__)
                    print("Fehler in Bild xpath Auslesen")
            try:
                item['images']= images
                yield loader.load_item()

            except Exception as e:
                print(e)
                
            yield loader.load_item()

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(SparkasseSpider, cls).from_crawler(
            crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed,
                                signal=signals.spider_closed)
        return spider

    # def getViertelIDFromLatLon(self, lat, lon, item):
    #     url = "https://nominatim.openstreetmap.org/reverse?format=jsonv2&lat=%s&lon=%s" % (
    #         lat, lon)
    #     r = requests.get(url=url)
    #     item["stadtvid"] = 0
    #     if not r:
    #         return
    #     nominatimresp = json.loads(r.content.decode('utf-8'))
       
    #     if 'address' in nominatimresp and 'suburb' in nominatimresp["address"]:
    #         stadtviertel = nominatimresp["address"]["suburb"]
    #         #print("STADTVIERTEL GEFUNDEN ALS: " + stadtviertel)
    #         stadtvid = self.extractor.extract (
    #              str(nominatimresp["address"]["road"]), 99, self.stadtid)
    #         item['stadtvid'] = stadtvid
    #         item["adresse"] = nominatimresp["address"]["road"]
    #     item["lat"] = lat
    #     item["lon"] = lon
           

        
            

    def spider_closed(self, spider, reason):
        print("SPIDER SPARKASSE " + self.stadtname +
                        "  CLOSED" + " REASON : " + reason)
        # self.db.setScrapedTime(self.conn, self.id)
        print(
            " scraped " + str(spider.crawler.stats.get_value('item_scraped_count')))
        # self.db.writeScrapStatistik(
        #     self.conn, 3, spider.crawler.stats.get_value('item_scraped_count'))
        # self.db.closeAllConnections(self.conn)
