#!/usr/bin/env python
# -*- coding: utf-8 -*-
import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from scrapy.http.request import Request
from demo_crawl.items import WGItem
from scrapy.loader import ItemLoader
from scrapy_splash import SplashRequest
import re
import json
from scrapy.exceptions import DropItem
from database import DataBase
from demo_crawl import settings as my_settings
import time
from ExtractViertel import ExtractViertel
#from fake_useragent import UserAgent
from scrapy import signals
import logging
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys

class WgsucheSpider(scrapy.Spider):

    name = 'wgsuche'
    stadtid = 0
    db = None
    conn = None
    stadt = ""
    custom_settings = { "CLOSESPIDER_ITEMCOUNT" : 150 }

    def __init__(self, stadtId, *args, **kwargs):
        self.db = DataBase()
        self.userToStadt = self.db.findStadtUrls(stadtId)
        if not self.userToStadt:
            print('USERTOSTADT IST NULL')
            return
        else:
            print('WG SUCHE MACHT ', self.userToStadt)
        self.extractor = ExtractViertel()
        self.extractor.init()

        super(WgsucheSpider, self).__init__(*args, **kwargs)

    def start_requests(self): 
        self.Kaufen = self.userToStadt.get("kaufen")
        self.Haus = self.userToStadt.get("haus")
        self.stadtid = self.userToStadt.get("stadtid")
        self.stadtname = self.userToStadt.get("stadtname")
        yield scrapy.Request(self.userToStadt['wgsuche'], callback=self.parse)



    def parse(self, response):
        jsonresponse = json.loads(response.body_as_unicode())
      
        for jsonitem in jsonresponse["result"]:
            item=WGItem()
            loader = ItemLoader(item, selector=response, response=response) 
            apiUrl = 'https://api.wg-suche.de/v1_0/offer/' +  str ( jsonitem["id"] ) 
            yield scrapy.Request(
                    apiUrl, callback=self.parse_images, meta={"item": item})
           
           
    def parse_images(self, response):
            try:
                transItem = response.meta["item"]
                jsonresponse = json.loads(response.body_as_unicode())

                loader = ItemLoader(transItem, selector=response, response=response)
                loader.add_value("title",jsonresponse["title"])
                transItem["haus"] = 2
                transItem["anbieter"] = "5"            
                transItem["url"] = "https://www.wg-suche.de/angebot/" + str ( jsonresponse["id"] )        
                transItem["stadtid"] = self.stadtid
           
                loader.add_value("gesamtkosten",jsonresponse["rent"])
                if "flatSize" in jsonresponse:
                    loader.add_value("gesamtflache",jsonresponse["flatSize"])
                if "size" in jsonresponse:
                    loader.add_value("zimmerflache",jsonresponse["size"])
                if "borough" in jsonresponse:
                    viertel =  jsonresponse['borough']
                    transItem['adresse'] = viertel
                if not 'adresse' in transItem:
                    transItem["adresse"] = ''
                if 'street' in jsonresponse:
                    transItem['adresse'] = transItem['adresse'] + ', ' + str(jsonresponse['street'])
                if 'streetNumber' in jsonresponse:
                    transItem['adresse'] = transItem['adresse'] + str(jsonresponse['streetNumber'])
             
                if "from" in jsonresponse:
                        loader.add_value("bezugsfreiab",jsonresponse["from"])
                if "membersWomanCount" in jsonresponse:
                    loader.add_value("anzahlf",jsonresponse["membersWomanCount"])
                if "membersManCount" in jsonresponse:
                    loader.add_value("anzahlm",jsonresponse["membersManCount"])
                if "wantedAmountFemale" in jsonresponse:
                    loader.add_value("gesuchtf",jsonresponse["wantedAmountFemale"])   
                if "wantedAmountMale" in jsonresponse:
                    loader.add_value("gesuchtm",jsonresponse["wantedAmountMale"])  
                if "wantedAmountEven" in jsonresponse:
                    loader.add_value("gesuchtm",1)  
                    loader.add_value("gesuchtf",1)  
                if "garden" in jsonresponse:   
                    loader.add_value("garten",jsonresponse["garden"])
                if "balcony" in jsonresponse:     
                    loader.add_value("balkon",jsonresponse["balcony"])
                if "elevator" in jsonresponse:     
                    loader.add_value("aufzug",jsonresponse["elevator"])
                if "barrierFree" in jsonresponse: 
                    loader.add_value("barriefrei",jsonresponse["barrierFree"])
                if "street" in jsonresponse and "streetNumber" in jsonresponse:  
                    loader.add_value("adresse",jsonresponse["street"] + " " + jsonresponse["streetNumber"] )
                if "street" in jsonresponse:
                    loader.add_value("adresse",jsonresponse["street"])
                if "furnished" in jsonresponse:  
                    loader.add_value("moebliert",jsonresponse["furnished"]  )    
                    transItem["images"] = []
                for image in jsonresponse["images"]:
                    transItem["images"].append(image["urls"]["ORIGINAL"]["url"])
                yield loader.load_item()
            except Exception as e:
                print(e)
                
    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(WgsucheSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def spider_closed(self, spider, reason):
        print("SPIDER WGSUCHE " + self.stadt + "  CLOSED" + " REASON : " + reason )     
        print( " scraped " + str (spider.crawler.stats.get_value('item_scraped_count') ))
      