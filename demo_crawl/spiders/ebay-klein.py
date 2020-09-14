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

class EbayKleinSpider(scrapy.Spider):
    custom_settings = {
        'CLOSESPIDER_ITEMCOUNT': '125'
    }

    name = 'ebay'
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
        super(EbayKleinSpider, self).__init__(*args, **kwargs)

    def start_requests(self):

        try:
            self.Kaufen = self.userToStadt["kaufen"]
            self.Haus = self.userToStadt["haus"]
            self.stadtid = self.userToStadt["stadtid"]
            self.stadtname = self.userToStadt["stadtname"]
            print( ("EBAY KLEIN mache url {}").format(self.userToStadt['ebay']))
            yield scrapy.Request(self.userToStadt['ebay'], callback=self.parse, meta={"stadtid": self.stadtid})
        except Exception as e:
            print(e)

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(EbayKleinSpider, cls).from_crawler(
            crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed,
                                signal=signals.spider_closed)
        return spider

    def parse(self, response):
        immos = response.xpath(
            "//a[@class='ellipsis']/@href").extract()
    
        stadtid = response.meta["stadtid"]
        for i in immos:
            try:
                url = 'https://www.ebay-kleinanzeigen.de' + i
                if self.db.checkIfInDupUrl(url) == False:
                    yield scrapy.Request(url=url, callback=self.parse_item, dont_filter=True, meta={"stadtid": stadtid})

            except Exception as e:
                print("ERROR IN EBAY KLEIN PARSE:" )
                traceback.print_exception(type(e), e, e.__traceback__)

        next_page = response.xpath(
            "//a[@class='pagination-next']/@href")
        if next_page:
            url =  self.userToStadt['ebay'] + str(next_page.get())
            yield scrapy.Request(url, self.parse, meta={"stadtid": self.stadtid})

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
                'title', "//h1[@id='viewad-title']/text()")
            
            if self.Kaufen == 0:
                loader.add_value('kaufen', '0')
            else:   
                loader.add_value('kaufen', '1')

            kosten = response.xpath("//h2[@id='viewad-price']/text()").get()
            if not self.hasNumbers(kosten):
                print('KEIN NUMBERS GEFUNDEN IN KOSTEN ' + str(kosten))
                return
            if '.' in str(kosten):
                kosten = kosten.replace('.','')
            loader.add_value('gesamtkosten', kosten)

            flache = response.xpath("//ul[@class='addetailslist']//text()[contains(.,'Wohnfläche')]/../span/text()").get()
            if '.' in str(flache):
                flache = flache.replace('.','')
            loader.add_value('flache', flache)
            loader.add_xpath('zimmer',"//ul[@class='addetailslist']//text()[contains(.,'Zimmer')]/../span/text()")
            loader.add_xpath('adresse', "//span[@id='viewad-locality']/text()")

            stadtid = response.meta["stadtid"]
            loader.add_value('stadtid', stadtid)
            loader.add_value('anbieter', "6")

            images = response.xpath("//div[contains(@class, 'galleryimage-element')]/img/@src").getall()
            item["images"] = []
            for image in images:
                try:
                   item["images"].append(image)
                except Exception as e:
                    traceback.print_exception(type(e), e, e.__traceback__)
                    print(e)  

            loader.add_xpath(
                'keller', "//ul[@class='checktaglist']//text()[contains(.,'Keller')]")
            loader.add_xpath(
                'garage', "//ul[@class='checktaglist']//text()[contains(.,'Garage')]")
            loader.add_xpath(
                'haustier', "//ul[@class='checktaglist']//text()[contains(.,'Haustiere erlaubt')]")
            loader.add_xpath(
                'barriefrei', "//ul[@class='checktaglist']//text()[contains(.,'Stufenloser Zugang')]")
            loader.add_xpath(
                'moebliert', "//ul[@class='checktaglist']//text()[contains(.,'Möbliert')]")
            loader.add_xpath(
                'ebk', "//ul[@class='checktaglist']//text()[contains(.,'Einbauküche')]")
            loader.add_xpath(
                'balkon', "//ul[@class='checktaglist']//text()[contains(.,'Balkon')]")
            loader.add_xpath(
                'terrasse', "//ul[@class='checktaglist']//text()[contains(.,'Terrasse')]")
            loader.add_xpath(
                'provisionsfrei', "//text()[contains(.,'Keine zusätzliche Käuferprovision')]")

            yield loader.load_item()

        except Exception as e:
            print("ERROR EBAYKLEIN IN PARSE ITEM:")
            print(traceback.print_exception(type(e), e, e.__traceback__))

    def spider_closed(self, spider):
        print("EBAY KLEIN scraped :" + str(spider.crawler.stats.get_value(
            'item_scraped_count')) + " IN DER STADT : " + str(self.stadtname))
      
