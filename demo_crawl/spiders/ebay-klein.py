#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from scrapy.http.request import Request
from demo_crawl.items import WGItem
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

class ImmonetSpider(scrapy.Spider):
    custom_settings = {
        'CLOSESPIDER_ITEMCOUNT': '125',
        'CLOSESPIDER_TIMEOUT': '60'

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
        super(ImmonetSpider, self).__init__(*args, **kwargs)

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
        spider = super(ImmonetSpider, cls).from_crawler(
            crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed,
                                signal=signals.spider_closed)
        return spider

    def parse(self, response):
        if self.stop:
            self.crawler.engine.close_spider(
                self, 'Zu Viele DUPLICATE URL Errors ')
            return

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
            print('NEXT URL IST ' + str(url))
            yield scrapy.Request(url, self.parse, meta={"stadtid": self.stadtid})

    def parse_item(self, response):
        try:
            item = WGItem()
            loader = ItemLoader(item, selector=response, response=response)

            loader.add_xpath(
                'title', "//h1[@id='viewad-title']/text()").get()  
            loader.add_xpath('gesamtkosten', "//h2[@id='viewad-price']/text()").get()  
            loader.add_xpath('zimmerflache', "(//span[@class='addetailslist--detail--value'])[2]/text()").get()  
            item["adresse"] = response.xpath("//span[@id='viewad-locality']/text()").get()

            
          
            images = []
            for i in range(1, 8):
                try:
                    # bil = 'bild%s' % (str(i))
                    xpath = '//div[@class="fotorama "]/div[%s]/@data-full' % (
                        str(i))
                    bildUrl = response.xpath(xpath).get()
                    if not bildUrl:
                        break
                    images.append(bildUrl)
                except Exception as e:
                    traceback.print_exception(type(e), e, e.__traceback__)
                    print("Fehler in Bild xpath Auslesen")

            item['images'] = images
           
          

            loader.add_xpath(
                'terrasse', "//span[contains(text(),'Terrasse')]/text()")
            loader.add_xpath(
                'balkon', "//span[contains(text(),'Balkon')]/text()")
            loader.add_xpath(
                'keller', "//span[contains(text(),'Keller')]/text()")
            loader.add_xpath(
                'garten', "//span[contains(text(),'Garten')]/text()")
            ebk = response.xpath(
                "//span[contains(text(),'EBK')]/text()").extract()
            if ebk:
                loader.add_value('ebk', "1")
            add = response.xpath(
                "normalize-space(//p[@class='text-100 pull-left']/text())").get()
            ort = response.xpath(
                "///p[@class='text-100 pull-left']/text()[preceding-sibling::br]").get()
            if ort:
                newAdd = str(add) + ', ' + \
                    str(ort)
                loader.add_value('adresse', newAdd.encode("utf-8"))
            else:
                add = add + ',' + response.meta["ortsviertel"]
                loader.add_value('adresse', str(add).encode("utf-8"))

            loader.add_xpath(
                'aufzug', "//span[contains(text(),'Personenaufzug')]/text()")
            loader.add_xpath(
                'barriefrei', "//span[contains(text(),'Barrierefrei')]/text()")
            loader.add_xpath(
                'mobliert', "//span[contains(text(),'Möbliert/Teilmöbliert')]/text()")

            stadtid = response.meta["stadtid"]

            ortsviertel = response.meta["ortsviertel"]
            if ortsviertel and str(ortsviertel).isalpha():
                stadtvid = self.extractor.extractAdresse(
                str(ortsviertel), 2, self.stadtid)
                if stadtvid and stadtvid != 0:
                    loader.add_value('stadtvid', stadtvid)

            else:
                loader.add_value('stadtvid', self.stadtvid)

            loader.add_value('stadtid', stadtid)
            loader.add_value('anbieter', "4")
            loader.add_value('stadtname', self.stadtname)

            yield loader.load_item()

        except Exception as e:
            print("ERROR IMMONET IN PARSE ITEM:")
            print(traceback.print_exception(type(e), e, e.__traceback__))

    def spider_closed(self, spider):
       # self.db.setScrapedTime(self.conn, self.id)
        print("IMMONET scraped :" + str(spider.crawler.stats.get_value(
            'item_scraped_count')) + " IN DER STADT : " + str(self.stadtname))
        # self.db.writeScrapStatistik(
        #     self.conn, 1, spider.crawler.stats.get_value('item_scraped_count'))
        # self.db.closeAllConnections(self.conn)
