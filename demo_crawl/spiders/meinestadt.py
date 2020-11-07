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
import time
from scrapy import signals
import logging
from scrapy.utils.log import configure_logging
import csv
from datetime import datetime
import signal
from database import DataBase
from ExtractViertel import ExtractViertel
import json
import traceback


class MeineStadtSpider(scrapy.Spider):
    custom_settings = {
        'CLOSESPIDER_ITEMCOUNT': '125'
    }

    name = 'meinestadt'
    Kaufen = 0
    Haus = 0
    stadtid = 0
    urlCounter = 1
    stadtname = ""
    standardurl = ""
    driver = None
    id = 0
    conn = None
    stop = False
    userToStadt = None
    extractor = None
    item = None

    def __init__(self, stadtId, *args, **kwargs):
        self.db = DataBase()
        # self.conn = self.db.create_conn()
        self.userToStadt = self.db.findStadtUrls(stadtId)
        self.extractor = ExtractViertel()
        self.extractor.init()
        super(MeineStadtSpider, self).__init__(*args, **kwargs)

    def start_requests(self):

        try:
            self.Kaufen = self.userToStadt["kaufen"]
            self.Haus = self.userToStadt["haus"]
            self.stadtid = self.userToStadt["stadtid"]
            self.stadtname = self.userToStadt["stadtname"]
            # self.stadtvid = self.userToStadt["StadtVid")
            print( ("MEINESTADT mache url {}").format(self.userToStadt['meinestadt']))

            yield scrapy.Request(self.userToStadt['meinestadt'], callback=self.parse)
        except Exception as e:
            print(e)

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(MeineStadtSpider, cls).from_crawler(
            crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed,
                                signal=signals.spider_closed)
        return spider

  
    def parse(self, response):
        
        if not response:
            print('ciao')
            return

        jsonresponse = json.loads(
            response.body.decode('utf-8'), encoding='utf-8')

        for jsonitem in jsonresponse["searchboxResults"]["items"]:
            try:
                self.item = ImmobilieItem()
                loader = ItemLoader(
                    self.item, selector=response, response=response)
                self.item["title"] = jsonitem["title"]
                self.item["url"] = jsonitem["detailUrl"]
                self.item["chatid"] = self.userToStadt["chatid"]
                self.item["zimmer"] = jsonitem["rooms"]
                self.item["flache"] = jsonitem["livingAreaRaw"]
                self.item["lat"] = jsonitem["latitude"]
                self.item["lon"] = jsonitem["longitude"]
                self.item["gesamtkosten"] = jsonitem["priceRaw"]    

                ausstattungsString = jsonitem["equipmentAsString"]
                if "Tiefgarage" in ausstattungsString:
                    self.item["garage"] = "1"

                if "Garten" in ausstattungsString:
                    self.item["garten"] = "1"

                if "Balkon" in ausstattungsString:
                    self.item["balkon"] = "1"

                if "Personenaufzug" in ausstattungsString:
                    self.item["aufzug"] = "1"

                if "Stellplatz" in ausstattungsString:
                    self.item["garage"] = "1"

                if "Terrasse" in ausstattungsString:
                    self.item["terrasse"] = "1"

                if "Einbauküche" in ausstattungsString:
                    self.item["ebk"] = "1"

                if "Kelleranteil" in ausstattungsString:
                    self.item["keller"] = "1"

                if "provisionsfrei" in ausstattungsString:
                    self.item["provisionsfrei"] = "1"

                if self.Haus == 1:
                    self.item["grundstuck"] = jsonitem["landAreaRaw"]

                loader.add_value('stadtid', self.stadtid)
                loader.add_value('anbieter', "2")
                loader.add_value('kaufen', self.Kaufen)
                loader.add_value('haus', self.Haus)
                loader.load_item()
                yield scrapy.Request(
                    jsonitem["detailUrl"], callback=self.parse_images, meta={"item": self.item})

            except Exception as ex:
                print('meinestadt error parse')
                print(ex)

    def parse_images(self, response):
        try:
            transItem = response.meta["item"]
            loader = ItemLoader(transItem, selector=response, response=response)
            if 'adresse' not in transItem:
                transItem['adresse'] = str(response.xpath(
                    "//div[ contains(@class, 'location')]/text()").get()).strip()
                if not transItem['adresse']:
                    transItem['adresse'] = response.xpath(
                        '//div[@class="a-resultListMetainfoItem__text "]/text()').get()
                loader.add_xpath('bezugsfreiab',
                                "//div[@class='section_content'][2]/p/text()")

            stadtvid = 0
            # if self.item['adresse']:
            #     stadtvid = self.extractor.extractAdresse(
            #          str(self.item['adresse']), 1, self.stadtid)
            # self.item['stadtvid'] = stadtvid

            bilder = response.xpath(
                "//div[ contains(@class,'m-gallery__imageContainer')]/img[contains(@class,'ImageNormal')]/@data-flickity-lazyload-src").getall()
            if bilder == None or len(bilder) == 0:
                bilder = response.xpath(
                    "//meta[ contains(@content, 'https://media-pics2.immowelt.org/')]/@content").getall()
            x = 1
            images = []

            for i in bilder:
                try:
                    if not i:
                        break
                    images.append(i)
                except Exception as e:
                    traceback.print_exception(type(e), e, e.__traceback__)
                    print("Fehler in Bild xpath Auslesen")
            try:
                transItem['images']=images
                print('load item ' + str(transItem))
                yield loader.load_item()

            except Exception as e:
                print(e)

        except Exception as ex:
            print(ex)

    def spider_closed(self, spider):
        #self.db.setScrapedTime(self.conn, self.id)

        print("MEINE STADT scraped :" + str(spider.crawler.stats.get_value(
            'item_scraped_count')) + " IN DER STADT : " + str(self.stadtname))
        # self.db.writeScrapStatistik(
        #     self.conn, 1, spider.crawler.stats.get_value('item_scraped_count'))
        # self.db.closeAllConnections(self.conn)
