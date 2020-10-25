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

class KalaySpider(scrapy.Spider):
    custom_settings = {
        'CLOSESPIDER_ITEMCOUNT': '125'
    }

    name = 'kalay'
    Kaufen = 0
    Haus = 0
    stadtid = 0
    stadtname = ""
    userToStadt = None
    extractor = None

    def __init__(self, stadtId, *args, **kwargs):
        self.db = DataBase()
        self.userToStadt = self.db.findStadtUrls(stadtId)
        self.extractor = ExtractViertel()
        self.extractor.init()
        super(KalaySpider, self).__init__(*args, **kwargs)

    def start_requests(self):

        try:
            self.Kaufen = self.userToStadt["kaufen"]
            self.Haus = self.userToStadt["haus"]
            self.stadtid = self.userToStadt["stadtid"]
            self.stadtname = self.userToStadt["stadtname"]
            print( ("KALAY mache url {}").format(self.userToStadt['ebay']))
            yield scrapy.Request(self.userToStadt['kalay'], callback=self.parse, meta={"stadtid": self.stadtid})
        except Exception as e:
            print(e)

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(KalaySpider, cls).from_crawler(
            crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed,
                                signal=signals.spider_closed)
        return spider

    def parse(self, response):
        immoUrls = response.xpath(
            "//div[@class='clear-row content-container']/a/@href").extract()
    
        stadtid = response.meta["stadtid"]
        for url in immoUrls:
            try:
                if self.db.checkIfInDupUrl(url) == False:
                    yield scrapy.Request(url=url, callback=self.parse_item, dont_filter=True, meta={"stadtid": stadtid})

            except Exception as e:
                print("ERROR IN KALAY PARSE:" )
                traceback.print_exception(type(e), e, e.__traceback__)

        next_page = response.xpath(
            "//a[@rel='next']/@href")
        if next_page:
            yield scrapy.Request(next_page.get(), self.parse, meta={"stadtid": self.stadtid})

    def hasNumbers(self, inputString):
        return any(char.isdigit() for char in inputString)

    def parse_item(self, response):
        try:
            item = ImmobilieItem()
            loader = ItemLoader(item, selector=response, response=response)
            item["url"] = response.url
            
            if self.Haus == 1:
                loader.add_value('haus', '1')
                loader.add_xpath(
                    'grundstuck', "//ul[@class='addetailslist']//text()[contains(.,'Grundstücksfläche')]/../span/text()")
            else:
                loader.add_value('haus', '0')
                
            loader.add_xpath(
                'title', "//h1[@id='exposeHeadline']/text()")
            
            kosten = None
            if self.Kaufen == 0:
                loader.add_value('kaufen', '0')
                kosten = response.xpath("(//text()[contains(.,'Warmmiete')])/../following-sibling::td/span/text()").get()
            else:   
                loader.add_value('kaufen', '1')
                kosten = response.xpath("(//text()[contains(.,'Kaufpreis')])/../following-sibling::td/span/text()").get()
                loader.add_xpath(
                'provisionsfrei', "//text()[contains(.,'Provisionsfrei')]")
                grundstucksfläche = response.xpath("//div[@class='general-info']//text()[contains(.,'Grundstücksfläche')]/../preceding-sibling::span/text()").get()

            if not kosten:
                print("KOSTEN NICHT GEFUNDEN : ", kosten )
                return 
            if not self.hasNumbers(kosten):
                print('KEIN NUMBERS GEFUNDEN IN KOSTEN ' + str(kosten))
                return
         
            loader.add_value('gesamtkosten', kosten)

            flache = response.xpath("//div[@class='general-info']//text()[contains(.,'Wohnfläche')]/../preceding-sibling::span/text()").get()
            if self.hasNumbers(flache):
                if ',' in str(flache):
                    flache = flache.split(',')[0]
                loader.add_value('flache', flache)
            
            loader.add_xpath('zimmer',"//div[@class='general-info']//text()[contains(.,'Zimmer')]/../preceding-sibling::span/text()")
            
            try:
                stadtPlz = response.xpath("(//table[@class='ad-info estate estate-content']//td[@class='label'])[1]/following-sibling::td/span/text()").get()
                strasse = response.xpath("(//table[@class='ad-info estate estate-content']//td[@class='label'])[2]/following-sibling::td/span/text()").get()
                adresse = strasse + stadtPlz
                if adresse:
                    loader.add_value('adresse', adresse)
            except Exception as e:
                print("FEHLER BEIM PARSEN DER ADRESSE IN KALY ", e)
           

            stadtid = response.meta["stadtid"]
            loader.add_value('stadtid', stadtid)
            loader.add_value('anbieter', "0")

            images = response.xpath("//img[contains(@class, 'gallery-cell')]/@data-flickity-lazyload").getall()
            item["images"] = []
            for image in images:
                try:
                    item["images"].append(image)
                except Exception as e:
                    traceback.print_exception(type(e), e, e.__traceback__)
                    print(e)  

            loader.add_xpath(
                'keller', "//table[contains(@class, 'ad-info estate  estate-content')]//text()[contains(.,'Keller')]")
            loader.add_xpath(
                'garage', "//table[contains(@class, 'ad-info estate  estate-content')]//text()[contains(.,'Garage')]")
            loader.add_xpath(
                'haustier', "//table[contains(@class, 'ad-info estate  estate-content')]//text()[contains(.,'Haustier')]")
            loader.add_xpath(
                'barriefrei', "//table[contains(@class, 'ad-info estate  estate-content')]//text()[contains(.,'barrierefrei')]")
            loader.add_xpath(
                'moebliert', "//table[contains(@class, 'ad-info estate  estate-content')]//text()[contains(.,'Möbliert')]")
            loader.add_xpath(
                'ebk', "//table[contains(@class, 'ad-info estate  estate-content')]//text()[contains(.,'Einbauküche')]")
            loader.add_xpath(
                'balkon', "//table[contains(@class, 'ad-info estate  estate-content')]//text()[contains(.,'Balkon')]")
            loader.add_xpath(
                'terrasse', "//table[contains(@class, 'ad-info estate  estate-content')]//text()[contains(.,'Terrasse')]")
          
            loader.add_xpath(
                'garten', "//table[contains(@class, 'ad-info estate  estate-content')]//text()[contains(.,'Garten')]")

            yield loader.load_item()

        except Exception as e:
            print("ERROR KALAY IN PARSE ITEM:")
            print(traceback.print_exception(type(e), e, e.__traceback__))

    def spider_closed(self, spider):
        print("KALAY scraped :" + str(spider.crawler.stats.get_value(
            'item_scraped_count')) + " IN DER STADT : " + str(self.stadtname))
      
