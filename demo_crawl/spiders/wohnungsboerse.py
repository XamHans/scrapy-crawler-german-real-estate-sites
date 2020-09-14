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

class WohnungsboerseSpider(scrapy.Spider):
    custom_settings = {
        'CLOSESPIDER_ITEMCOUNT': '125'    }

    name = 'wohnungsboerse'
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
        super(WohnungsboerseSpider, self).__init__(*args, **kwargs)

    def start_requests(self):

        try:
            self.Kaufen = self.userToStadt["kaufen"]
            self.Haus = self.userToStadt["haus"]
            self.stadtid = self.userToStadt["stadtid"]
            self.stadtname = self.userToStadt["stadtname"]
            # self.stadtvid = self.userToStadt["StadtVid")
            print( ("wohnungsboerse mache url {}").format(self.userToStadt['wohnungsboerse']))

            yield scrapy.Request(self.userToStadt['wohnungsboerse'], callback=self.parse, meta={"stadtid": self.stadtid})
        except Exception as e:
            print(e)
            
    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(WohnungsboerseSpider, cls).from_crawler(crawler, *args, **kwargs)
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
                "//a[contains(@href,'immodetail')]")
            stadtid = response.meta["stadtid"]

            for i in immos:
                url = i.xpath("@href").get()
                image = i.xpath("img/@data-src").get()
                if not "wohnungsboerse" in url:
                    url = 'https://www.wohnungsboerse.net' + url
             
                if self.db.checkIfInDupUrl(url) == False:
                    yield scrapy.Request(url=url, callback=self.parse_item, dont_filter=True,
                                        meta={"stadtid": stadtid, "url": url, "imageurl":image})
            
            # next_page = response.xpath(
            #     "//a[@class='nextLink slink']/@href").get()
            # if next_page:
            #     url = response.urljoin(next_page)
            #     yield scrapy.Request(url, self.parse, meta={"stadtid": self.stadtid})

        except Exception as e:
            print("ERROR IN wohnungsboerse PARSE:")
            traceback.print_exception(type(e), e, e.__traceback__)

    def parse_item(self, response):
        try:
            item = ImmobilieItem()
            loader = ItemLoader(item, selector=response, response=response)
            loader.add_xpath(
                'title', "//h2[@class='dotdotdot']/text()")
            item['url'] = response.meta["url"]
            imageurl = response.meta["imageurl"]

            bilder = response.xpath("//img[contains(@src, 'https://cdn.wohnungsboerse.net/img/thumbs')]/@src").getall()
            images = []
            images.append(imageurl)
            for i in bilder:
                try:
                    print(str(i))
                    images.append(i)
                except:
                    print("Fehler in Bild xpath Auslesen")
            try:
                item['images'] = images
            except Exception as e:
                logging.warning('fehler bei zuwesien von images to  item :' +str(e))
                
            loader.add_xpath(
                'zimmer', "//dt[contains(text(),'ZIMMER')]//ancestor::dl/dd/text()")
            loader.add_xpath(
                'flache', "//dt[contains(text(),'FLÄCHE')]//ancestor::dl/dd/text()")

            
            if self.Kaufen == 0:
                
                loader.add_value('kaufen', '0')    
                gesamtk = response.xpath("//div/b[contains(text(),'Gesamt')]/../following-sibling::div[1]/div/text()").get()
               
                loader.add_value('gesamtkosten', gesamtk)
            else:
                loader.add_value('kaufen', '1')
                loader.add_xpath(
                    'gesamtkosten', "//dt[contains(text(),'KAUFPREIS')]/following-sibling::dd[1]/text()")
                provisionfrei = response.xpath("//text()[contains(.,'Provision')]").get()
                if provisionfrei:
                    print(provisionfrei)
                    loader.add_value('provisionsfrei', "1")

            if self.Haus == 1:
                loader.add_value('haus', '1')
                loader.add_xpath(
                    'grundstuck', "//div[contains(text(),'Grundstücksfläche:')]//following-sibling::div[1]/text()")
            else:
                loader.add_value('haus', '0')
           
            loader.add_xpath(
                'keller', "//text()[contains(.,'Keller')]")
            loader.add_xpath(
                'balkon', "//text()[contains(.,'Balkon')]")
            loader.add_xpath(
                'garage', "//text()[contains(.,'Garage')]")
            loader.add_xpath(
                'haustier', "//text()[contains(.,'Haustiere erlaubt')]")
            loader.add_xpath(
                'barriefrei', "//text()[contains(.,'Stufenloser Zugang')]")
            loader.add_xpath(
                'moebliert', "//text()[contains(.,'Möbliert')]")    
            loader.add_xpath(
                'terrasse', "//text()[contains(.,'Terrasse')]")
            
            add = response.xpath(
                    "//div[@class='mb-2 mb-lg-3 pl-3 pl-lg-0']/text()").get()
         
            if add:
                loader.add_value('adresse', str(add).encode("utf-8"))
            

            loader.add_value('stadtid', self.stadtid)
            loader.add_value('anbieter', "9")
            loader.add_value('stadtname', self.stadtname)

            yield loader.load_item()

        except Exception as e:
            print("ERROR wohnungsboerse IN PARSE ITEM:")
            traceback.print_exception(type(e), e, e.__traceback__)

    def spider_closed(self, spider):
        print("WOHNUNGSBOERSE scraped :" + 
        str(spider.crawler.stats.get_value('item_scraped_count')))
