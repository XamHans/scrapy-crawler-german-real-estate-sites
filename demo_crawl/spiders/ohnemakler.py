#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from scrapy.http.request import Request
from demo_crawl.items import ImmobilieItem
from scrapy.loader import ItemLoader
import re
#from importlib import reload
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

class ImmoSpider(scrapy.Spider):
    custom_settings = {
        'CLOSESPIDER_ITEMCOUNT': '125'
            }

    name = 'ohnemakler'
    conn = None
    Kaufen = 0
    Haus = 0
    stadtid = 0
    stop = False
    userToStadt = None
    extractor = None
    stadtname = None

    def __init__(self, stadtId, *args, **kwargs):
        self.db = DataBase()
        # self.conn = self.db.create_conn()
        print('stadtid ist ' + str(stadtId))
        self.userToStadt = self.db.findStadtUrls(stadtId)
        if not self.userToStadt:
            print('USERTOSTADT IST NULL')
        else:
            print('bearbeite ', self.userToStadt)
        self.extractor = ExtractViertel()
        self.extractor.init()
        super(ImmoSpider, self).__init__(*args, **kwargs)

    def start_requests(self):

        try:
            print('inside start_requests')
            self.Kaufen = self.userToStadt["kaufen"]
            self.Haus = self.userToStadt["haus"]
            self.stadtid = self.userToStadt["stadtid"]
            self.stadtname = self.userToStadt["stadtname"]
            # self.stadtvid = self.userToStadt["StadtVid")
            print( ("ohnemakler mache url {}").format(self.userToStadt['ohnemakler']))

            yield scrapy.Request(self.userToStadt['ohnemakler'], callback=self.parse, meta={"stadtid": self.stadtid})
        except Exception as e:
            print(e)
            
    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(ImmoSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed,
                                signal=signals.spider_closed)
        return spider

    def parse(self, response):
        try:
            if self.stop:
                self.crawler.engine.close_spider(
                    self, 'Zu Viele DUPLICATE URL Errors ')
                return

            immos = response.xpath(
                "//a[@class='red']/@href").extract()
            stadtid = response.meta["stadtid"]

            for i in immos:
                url = 'https://www.ohne-makler.net/' + i
                if self.db.checkIfInDupUrl(url) == False:
                    yield scrapy.Request(url=url, callback=self.parse_item, dont_filter=True, meta={"stadtid": stadtid, "url": url})
            
                next_page = response.xpath(
                    "//li[@class='next']/a/@href").get()
                if next_page:
                    url = response.urljoin(next_page)
                    print('URL IST ' + str(url))
                    yield scrapy.Request(url, self.parse, meta={"stadtid": self.stadtid})

        except Exception as e:
            print("ERROR IN OHNEMAKLER PARSE:")
            traceback.print_exception(type(e), e, e.__traceback__)

    def parse_item(self, response):
        try:
            item = ImmobilieItem()
            loader = ItemLoader(item, selector=response, response=response)
          
            loader.add_xpath(
                'title', "//h1[@class='blue']/text()")
            item['url'] = response.meta["url"]

            bilder = response.xpath("//a[contains(@class, 'pictures')]/@href").getall()
            images = []
            for i in bilder:
                try:
                    bild = 'https://www.ohne-makler.net' + i
                    images.append(bild)
                except:
                    print("Fehler in Bild xpath Auslesen")
            try:
                item['images'] = images
            except Exception as e:
                logging.warning('fehler bei zuwesien von images to  item :' +str(e))
                
            zimmer = response.xpath("//text()[contains(.,'Zimmer (Anzahl)')]/ancestor::p/text()[2]").get()
            loader.add_value('zimmer', zimmer)  

            flache = response.xpath("//text()[contains(.,'Wohnfläche')]/ancestor::p/text()[2]").get()
            loader.add_value('flache', flache)  
            
            if self.Kaufen == 0:
                
                loader.add_value('kaufen', '0')    
                gesamtk = response.xpath("//text()[contains(.,'Pauschal')]/ancestor::p/span/text()").get()
                if not gesamtk:
                    kaltmiete = response.xpath("//text()[contains(.,'Kaltmiete')]/ancestor::p/span/text()").get().replace('€','').replace('.','').strip()
                    print('KALTMIETE IST '+ str(kaltmiete))
                    
                    nebenkosten = response.xpath("//text()[contains(.,'Nebenkosten')]/ancestor::p/text()[2]").get().replace('€','').replace('.','').strip()
                    print('SUMME NEBENKOSTEN IST '+ str(nebenkosten))
                    
                    gesamtk = float(kaltmiete) + float(nebenkosten)
                print('GESAMTK IST '+ str(int(round(gesamtk))))
                loader.add_value('gesamtkosten', str(int(round(gesamtk))))
         

            else:
                loader.add_value('kaufen', '1')
                loader.add_xpath(
                    'gesamtkosten', "//text()[contains(.,'Kaufpreis')]/ancestor::p/span/text()")
                loader.add_xpath('provisionsfrei', "//text()[contains(.,'provisionsfrei')]")

            if self.Haus == 1:
                loader.add_value('haus', '1')
                loader.add_xpath(
                    'grundstuck', "//text()[contains(.,'Grundstücksfläche')]/ancestor::p/text()[2]")
            else:
                loader.add_value('haus', '0')
           
            loader.add_xpath(
                'keller', "//text()[contains(.,'Keller')]")
            loader.add_xpath(
                'balkon', "//text()[contains(.,'Balkon')]")
            loader.add_xpath(
                'garage', "//text()[contains(.,'Garage')]")
            loader.add_xpath(
                'garten', "//text()[contains(.,'Garten')]")
            loader.add_xpath(
                'ebk', "//text()[contains(.,'Einbauküche')]")
            loader.add_xpath(
                'haustier', "//text()[contains(.,'Haustiere erlaubt')]")
            loader.add_xpath(
                'barriefrei', "//text()[contains(.,'Barrierefrei')]")
            loader.add_xpath(
                'moebliert', "//text()[contains(.,'Möbliert')]")    
            loader.add_xpath(
                'terrasse', "//text()[contains(.,'Terrasse')]")
            
            add = response.xpath("//div[@class='span4'][3]//h4/following::p[1]/text()").get().strip()
         
            if add:
                loader.add_value('adresse', str(add).encode("utf-8"))
            

            loader.add_value('stadtid', self.stadtid)
            loader.add_value('anbieter', "8")
            loader.add_value('stadtname', self.stadtname)

            yield loader.load_item()

        except Exception as e:
            print("ERROR OHNEMAKLER IN PARSE ITEM:")
            traceback.print_exception(type(e), e, e.__traceback__)

    def spider_closed(self, spider):
        print("OHNEMAKLER  scraped :" + 
        str(spider.crawler.stats.get_value('item_scraped_count')))
