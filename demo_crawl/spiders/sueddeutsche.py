#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from scrapy.http.request import Request
from demo_crawl.items import ImmobilieItem, WGItem
from scrapy.loader import ItemLoader
import time
from scrapy import signals
import logging
from scrapy.utils.log import configure_logging
from datetime import datetime
import signal
from database import DataBase
import traceback
import re
import json


class SueddeutscheSpider(scrapy.Spider):
    custom_settings = {
        'CLOSESPIDER_ITEMCOUNT': '125'
    }

    name = 'sueddeutsche'
    Kaufen = 0
    Haus = 0
    stadtid = 0
    stadtname = ""
    userToStadt = None
    extractor = None

    def __init__(self, stadtId, *args, **kwargs):
        self.db = DataBase()
        self.userToStadt = self.db.findStadtUrls(stadtId)
        super(SueddeutscheSpider, self).__init__(*args, **kwargs)

    def start_requests(self):

        try:
            self.Kaufen = self.userToStadt["kaufen"]
            self.Haus = self.userToStadt["haus"]
            self.stadtid = self.userToStadt["stadtid"]
            self.stadtname = self.userToStadt["stadtname"]
            print( ("Sueddeutsche mache url {}").format(self.userToStadt['sueddeutsche']))
            yield scrapy.Request(self.userToStadt['sueddeutsche'], callback=self.parse, meta={"stadtid": self.stadtid})
        except Exception as e:
            print(e)

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(SueddeutscheSpider, cls).from_crawler(
            crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed,
                                signal=signals.spider_closed)
        return spider
    
    def hasNumbers(self, inputString):
        return any(char.isdigit() for char in inputString)
    
    def parse(self, response):
    
        stadtid = response.meta["stadtid"]
        jsonresponse = json.loads(response.body.decode('utf-8'), encoding='utf-8')

        for jsonitem in jsonresponse["searchResult"]["result"]: 
            try:
                url = 'https://immobilienmarkt.sueddeutsche.de' + jsonitem["url"]
                if self.db.checkIfInDupUrl(url) == False:
                    yield scrapy.Request(url=url, callback=self.parse_item, dont_filter=True, meta={"stadtid": stadtid})

            except Exception as e:
                print("ERROR IN sueddeutsche PARSE:" )
                traceback.print_exception(type(e), e, e.__traceback__)

    def parse_item(self, response):
        try:
            item = ImmobilieItem()
            loader = ItemLoader(item, selector=response, response=response)
            item["url"] = response.url
            if 'chatid' in self.userToStadt:
                item["chatid"] = self.userToStadt["chatid"]
            if self.Haus == 1:
                loader.add_value('haus', '1')
                loader.add_xpath(
                    'grundstuck', "(//td[@class='firstTd2']/following-sibling::td/div/text())[2]")
            else:
                loader.add_value('haus', '0')
                
            loader.add_xpath(
                'title', "//*[@class='exposeTitle']/text()")
            
            kosten = None
            if self.Kaufen == 0:
                loader.add_value('kaufen', '0')
                # here warm miete text ergänzen ansonten such nach kaltmiete
                kosten = response.xpath("(//text()[contains(.,'Miete inkl. NK')])/../../following-sibling::td/div/text()").get()
                if not kosten:
                    kosten = response.xpath("(//text()[contains(.,'Miete zzgl. NK')])/../../following-sibling::td/div/text()").get()

            else:   
                loader.add_value('kaufen', '1')
                kosten = response.xpath("(//text()[contains(.,'Kaufpreis')])/../../following-sibling::td/div/text()").get()
                provision = response.xpath("//text()[contains(.,'Provisionsfrei')]").get()
                if  provision:
                    loader.add_value(
                    'provisionsfrei', "1")

            if not kosten:
                print("KOSTEN NICHT GEFUNDEN : ", kosten )
                print("CHECK SELBST: ", response.url)
                return 
            
            if not self.hasNumbers(kosten):
                print('KEIN NUMBERS GEFUNDEN IN KOSTEN ' + str(kosten))
                return
         
            loader.add_value('gesamtkosten', kosten)

            flache = response.xpath("(//text()[contains(.,'Wohnfläche')])/../../../div[@class='value']/text()").get()
            if self.hasNumbers(flache):
                if ',' in str(flache):
                    flache = flache.split(',')[0]
                loader.add_value('flache', flache)
            
            loader.add_xpath('zimmer',"(//text()[contains(.,'Zimmeranzahl')])/../../div[@class='value']/text()")
            
            try:
                adresse = ''
                plz = response.xpath("(//div[@class='exposeAddr']/div/span/text())[1]").get()
                bezirk = response.xpath("(//div[@class='exposeAddr']/div/span/text())[4]").get()
               
                if bezirk:
                    adresse =  bezirk
                if plz:
                    adresse = adresse + ' (' + str(plz).replace(',','') + ') '
                if adresse:
                    loader.add_value('adresse', adresse)
            except Exception as e:
                print("FEHLER BEIM PARSEN DER ADRESSE IN KALY ", e)
           

            stadtid = response.meta["stadtid"]
            loader.add_value('stadtid', stadtid)
            loader.add_value('anbieter', "11")

            images = response.xpath("//ul[@class='imageSliderThumbs']//img/@src").getall()
            item["images"] = []
            for image in images:
                try:
                    item["images"].append(image)
                except Exception as e:
                    traceback.print_exception(type(e), e, e.__traceback__)
                    print(e)  

            loader.add_xpath(
                'keller', "//div[@class='hideContentInner']//text()[contains(.,'Keller')]")
            loader.add_xpath(
                'garage', "//div[@class='hideContentInner']//text()[contains(.,'Garage')]")
            loader.add_xpath(
                'haustier', "//div[@class='hideContentInner']//text()[contains(.,'Haustier')]")
            loader.add_xpath(
                'barriefrei', "//div[@class='hideContentInner']//text()[contains(.,'barrierefrei')]")
            loader.add_xpath(
                'moebliert', "//div[@class='hideContentInner']//text()[contains(.,'Möbliert')]")
            loader.add_xpath(
                'ebk', "//div[@class='hideContentInner']//text()[contains(.,'Einbauküche')]")
            loader.add_xpath(
                'balkon', "//div[@class='hideContentInner']//text()[contains(.,'Balkon')]")
            loader.add_xpath(
                'terrasse', "//div[@class='hideContentInner']//text()[contains(.,'Terrasse')]")
          
            loader.add_xpath(
                'garten', "//div[@class='hideContentInner']//text()[contains(.,'Garten')]")

            yield loader.load_item()

        except Exception as e:
            print("ERROR sueddeutsche IN PARSE ITEM:")
            print(traceback.print_exception(type(e), e, e.__traceback__))

    def spider_closed(self, spider):
        print("sueddeutsche scraped :" + str(spider.crawler.stats.get_value(
            'item_scraped_count')) + " IN DER STADT : " + str(self.stadtname))
      
