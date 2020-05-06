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
            loader.add_value("title",jsonitem["title"])
            loader.add_value("gesamtkosten",jsonitem["rent"])
            if "size" in jsonitem:
                loader.add_value("flache",jsonitem["size"])
            else:
                raise DropItem("Missing flatsize in %s" % item)    
            loader.add_value("anbieter","5")
            if "from" in jsonitem:
                loader.add_value("bezugsfreiab",jsonitem["from"])
            if "membersWomanCount" in jsonitem:
                loader.add_value("anzahlf",jsonitem["membersWomanCount"])
            if "membersManCount" in jsonitem:
                loader.add_value("anzahlm",jsonitem["membersManCount"])
            if "wantedAmountFemale" in jsonitem:
                loader.add_value("gesuchtf",jsonitem["wantedAmountFemale"])   
            if "wantedAmountMale" in jsonitem:
                loader.add_value("gesuchtm",jsonitem["wantedAmountMale"])  
            if "garden" in jsonitem:   
                loader.add_value("garten",jsonitem["garden"])
            if "balcony" in jsonitem:     
                loader.add_value("balkon",jsonitem["balcony"])
            if "elevator" in jsonitem:     
                loader.add_value("aufzug",jsonitem["elevator"])
            if "barrierFree" in jsonitem: 
                loader.add_value("barriefrei",jsonitem["barrierFree"])
            if "street" in jsonitem and "streetNumber" in jsonitem:  
                loader.add_value("adresse",jsonitem["street"] + " " + jsonitem["streetNumber"] )
            if "street" in jsonitem:
                loader.add_value("adresse",jsonitem["street"])
            if "furnished" in jsonitem:  
                loader.add_value("moebliert",jsonitem["furnished"]  )    
            loader.add_value("url","https://www.wg-suche.de/angebot/" + str ( jsonitem["id"] ) )
            loader.add_value("stadtid",self.stadtid)

            if "borough" in jsonitem:
                viertel =  jsonitem['borough']
                item['adresse'] = viertel

            try:
                images = []
                images.append(jsonitem["image"]["urls"]["XL"]["url"])
                item['images'] = images
                yield loader.load_item()

            except Exception as e:
                print(e)
                
        
        return loader.load_item()

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(WgsucheSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def spider_closed(self, spider, reason):
        print("SPIDER WGSUCHE " + self.stadt + "  CLOSED" + " REASON : " + reason )     
        print( " scraped " + str (spider.crawler.stats.get_value('item_scraped_count') ))
      