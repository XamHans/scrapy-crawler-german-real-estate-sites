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
import csv
from datetime import datetime
import signal
from database import DataBase
from ExtractViertel import ExtractViertel
import traceback
import re

class BerlinSpider(scrapy.Spider):
    custom_settings = {
        'CLOSESPIDER_ITEMCOUNT': '125'
    }

    name = 'berlin'
    Kaufen = 0
    Haus = 0
    stadtid = 0
    stadtname = ""
    userToStadt = None
    extractor = None

    def __init__(self, stadtId, *args, **kwargs):
        self.db = DataBase()
        self.userToStadt = self.db.findStadtUrls(stadtId)
        super(BerlinSpider, self).__init__(*args, **kwargs)

    def start_requests(self):

        try:
            self.Kaufen = self.userToStadt["kaufen"]
            self.Haus = self.userToStadt["haus"]
            self.stadtid = self.userToStadt["stadtid"]
            self.stadtname = self.userToStadt["stadtname"]
            print( ("berlin mache url {}").format(self.userToStadt['berlin']))
            yield scrapy.Request(self.userToStadt['berlin'], callback=self.parse, meta={"stadtid": self.stadtid})
        except Exception as e:
            print(e)

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(BerlinSpider, cls).from_crawler(
            crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed,
                                signal=signals.spider_closed)
        return spider

    def parse(self, response):
        immoUrls = response.xpath(
            "//article/h3/a/@href").extract()
    
        stadtid = response.meta["stadtid"]
        for url in immoUrls:
            try:
                url = 'https://www.berlin.de' + url
                if self.db.checkIfInDupUrl(url) == False:
                    yield scrapy.Request(url=url, callback=self.parse_item, dont_filter=True, meta={"stadtid": stadtid})

            except Exception as e:
                print("ERROR IN berlin PARSE:" )
                traceback.print_exception(type(e), e, e.__traceback__)

        next_page = response.xpath(
            "(//a[@rel='next']/@href)[2]").get()
        if next_page:
            next_page = 'https://www.berlin.de' + next_page
            yield scrapy.Request(next_page, self.parse, meta={"stadtid": self.stadtid})

    def hasNumbers(self, inputString):
        return any(char.isdigit() for char in inputString)

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
                    'grundstuck', "(//text()[contains(.,'Gesamtfläche')])/../../span[@class='text']/text()")
            else:
                loader.add_value('haus', '0')
                
            loader.add_xpath(
                'title', "//*[@class='heading--article']/text()")
            
            kosten = None
            if self.Kaufen == 0:
                loader.add_value('kaufen', '0')
                kosten = response.xpath("(//text()[contains(.,'Warm-Miete')])/../../span[@class='text']/text()").get()
                if not kosten:
                    kosten = response.xpath("(//text()[contains(.,'Netto-Kaltmiete')])/../../span[@class='text']/text()").get()
            else:   
                loader.add_value('kaufen', '1')
                kosten = response.xpath("(//text()[contains(.,'Kaufpreis')])[2]/../../span[@class='text']/text()").get()
                provision = response.xpath("(//text()[contains(.,'Courtage:')])xt']/text()").get()
                if not provision:
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

            flache = response.xpath("(//text()[contains(.,'Wohnfläche')])/../../span[@class='text']/text()").get()
            if self.hasNumbers(flache):
                if ',' in str(flache):
                    flache = flache.split(',')[0]
                loader.add_value('flache', flache)
            
            loader.add_xpath('zimmer',"(//text()[contains(.,'Zimmer')])/../../span[@class='text']/text()")
            
            try:
                strasse = response.xpath("//*[@class='street-address']/text()").get()
                plz = response.xpath("//*[@class='postal-code']/text()").get()
                bezirk = response.xpath("//*[@class='locality']/text()").get()
                if strasse:
                    adresse = strasse + ', '
                if bezirk:
                    adresse = adresse + ' ' + bezirk
                if plz:
                    adresse = adresse + ' (' + plz + ') '
                if adresse:
                    loader.add_value('adresse', adresse)
            except Exception as e:
                print("FEHLER BEIM PARSEN DER ADRESSE IN KALY ", e)
           

            stadtid = response.meta["stadtid"]
            loader.add_value('stadtid', stadtid)
            loader.add_value('anbieter', "10")

            images = response.xpath("//img[contains(@data-src, 'immobilienscout24.de')]/@data-src").getall()
            item["images"] = []
            for image in images:
                try:
                    item["images"].append(image)
                except Exception as e:
                    traceback.print_exception(type(e), e, e.__traceback__)
                    print(e)  

            loader.add_xpath(
                'keller', "//div[@id='expose']//text()[contains(.,'Keller')]")
            loader.add_xpath(
                'garage', "//div[@id='expose']//text()[contains(.,'Garage')]")
            loader.add_xpath(
                'haustier', "//div[@id='expose']//text()[contains(.,'Haustiere')]")
            loader.add_xpath(
                'barriefrei', "//div[@id='expose']//text()[contains(.,'barrierefrei')]")
            loader.add_xpath(
                'moebliert', "//div[@id='expose']//text()[contains(.,'Möbliert')]")
            loader.add_xpath(
                'ebk', "//div[@id='expose']//text()[contains(.,'Einbauküche')]")
            loader.add_xpath(
                'balkon', "//div[@id='expose']//text()[contains(.,'Balkon')]")
            loader.add_xpath(
                'terrasse', "//div[@id='expose']//text()[contains(.,'Terrasse')]")
          
            loader.add_xpath(
                'garten', "//div[@id='expose']//text()[contains(.,'Garten')]")

            yield loader.load_item()

        except Exception as e:
            print("ERROR berlin IN PARSE ITEM:")
            print(traceback.print_exception(type(e), e, e.__traceback__))

    def spider_closed(self, spider):
        print("berlin scraped :" + str(spider.crawler.stats.get_value(
            'item_scraped_count')) + " IN DER STADT : " + str(self.stadtname))
      
