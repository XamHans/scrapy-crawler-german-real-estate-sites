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
        'CLOSESPIDER_ITEMCOUNT': '125',
        'CLOSESPIDER_TIMEOUT': '60'
    }

    name = 'immoscout'
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
            return
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
            print( ("IMMOSCOUT mache url {}").format(self.userToStadt['immoscout']))

            yield scrapy.Request(self.userToStadt['immoscout'], callback=self.parse, meta={"stadtid": self.stadtid})
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

            if response.status == 410:
                print("HALLO 404")
                yield scrapy.Request(self.standardurl, callback=self.parse, meta={"stadtid": self.stadtid})

            immos = response.xpath(
                "//a[@class='result-list-entry__brand-title-container']/@href").extract()
            stadtid = response.meta["stadtid"]

            for i in immos:
                if 'https://' in str(i):
                    continue
                url = 'https://www.immobilienscout24.de' + i
                if self.db.checkIfInDupUrl(url) == False:
                    yield scrapy.Request(url=url, callback=self.parse_item, dont_filter=True, meta={"stadtid": stadtid})
            
                next_page = response.xpath(
                    "//a[@data-is24-qa='paging_bottom_next']/@href")
                if next_page:
                    url = response.urljoin(next_page[0].extract())
                    yield scrapy.Request(url, self.parse, meta={"stadtid": self.stadtid})

        except Exception as e:
            print("ERROR IN IMMOSCOUT PARSE:")
            traceback.print_exception(type(e), e, e.__traceback__)

    def parse_item(self, response):
        try:
            item = ImmobilieItem()
            loader = ItemLoader(item, selector=response, response=response)

            for info in response.xpath("//div[@class='criteriagroup criteria-group--two-columns']"):
                loader = ItemLoader(item, selector=info, response=response)
                loader.add_xpath(
                    'title', "//h1[@id='expose-title']/text()")
                loader.add_xpath(
                    'typ', ".//dd[@class='is24qa-typ grid-item three-fifths']/text()")
                loader.add_xpath(
                    'bezugsfreiab', ".//dd[@class='is24qa-bezugsfrei-ab grid-item three-fifths']/text()")
                loader.add_xpath(
                    'haustier', ".//dd[@class='is24qa-haustiere grid-item three-fifths']/text()")
                garage = response.xpath(
                    ".//dd[@class='is24qa-garage-stellplatz grid-item three-fifths']/text()").extract()
                if garage:
                    loader.add_xpath('garage', "1")
                loader.add_xpath(
                    'url', "//link[@rel='canonical']/@href")

            loader.load_item()
            images = []
            for i in range(1, 8):
                try:
                    bild = response.xpath('(//img[@class=\'sp-image \']/@data-src)[%s]' % (str(i))).extract()
                    images.append(bild)
                except:
                    print("Fehler in Bild xpath Auslesen")
            try:
                item['images'] = images
            except Exception as e:
                logging.warning('fehler bei zuwesien von images to  item :' +str(e))

            loader.add_xpath(
                'zimmer', "//dd[@class='is24qa-zimmer grid-item three-fifths']/text()")

            if self.Kaufen == 0:
                loader.add_value('kaufen', '0')
              
                kaltmiete = response.xpath("//div[@class='is24qa-kaltmiete is24-value font-semibold']/text()").get()
                loader.add_value('kaltmiete', kaltmiete)
                flache = response.xpath("//div[@class='is24qa-flaeche is24-value font-semibold']/text()").get()
                if flache:
                    if ',' in flache:
                        flache = str(flache).split(',')[0]
                   
                loader.add_value(
                    'flache', flache)
                loader.add_xpath(
                    'nebenkosten', "//dd[@class='is24qa-nebenkosten grid-item three-fifths']/text()[2]")
                try:
                    gesamtk = response.xpath("//dd[@class='is24qa-gesamtmiete grid-item three-fifths font-bold']/text()").get()
                    if not gesamtk:
                	    gesamtk = kaltmiete
                    loader.add_value('gesamtkosten', gesamtk)
                except Exception:
                    pass
                loader.add_xpath(
                    'gesamtkosten', "//dd[@class='is24qa-gesamtmiete grid-item three-fifths font-bold']/text()")

            else:
                loader.add_value('kaufen', '1')
                loader.add_xpath(
                    'gesamtkosten', "//div[@class='is24qa-kaufpreis is24-value font-semibold']/text()")
                loader.add_xpath(
                    'flache', "//dd[@class='is24qa-wohnflaeche-ca grid-item three-fifths']/text()")
                loader.add_xpath(
                    'provisionsfrei', "//span[@class='is24qa-provisionsfrei-label']/text()")

            if self.Haus == 1:
                loader.add_value('haus', '1')
                loader.add_xpath(
                    'grundstuck', "//dd[@class='is24qa-grundstueck-ca grid-item three-fifths']/text()")
            else:
                loader.add_value('haus', '0')

            loader.load_item()
            add = ""

            for features in response.xpath("//div[@class='criteriagroup boolean-listing padding-top-l']"):
                loader = ItemLoader(
                    item, selector=features, response=response)
                loader.add_xpath(
                    'terrasse', ".//span[@class='is24qa-balkon-terrasse-label']/text()")
                loader.add_xpath(
                    'balkon', ".//span[@class='is24qa-balkon-terrasse-label']/text()")
                loader.add_xpath(
                    'keller', ".//span[@class='is24qa-keller-label']/text()")
                loader.add_xpath(
                    'garten', ".//span[@class='is24qa-garten-mitbenutzung-label']/text()")
                ebk = response.xpath(
                    "//span[@class='is24qa-einbaukueche-label']/text()").extract()
                if ebk:
                    loader.add_value('ebk', "1")
               
                loader.add_xpath(
                    'aufzug', ".//span[@class='is24qa-personenaufzug-label']/text()")
                loader.add_xpath(
                    'barriefrei', ".//span[@class='is24qa-stufenloser-zugang-label']/text()")

            add = response.xpath(
                    "//span[@class='block font-nowrap print-hide']/text()").extract()
            viertel = response.xpath('//ul[@class="breadcrumb__item--current"]/preceding::a[1]').get()
            loader.add_value(
                'ort', viertel)
            if add:
                add = str(add) + ', ' + str(viertel)
                loader.add_value('adresse', str(add).encode("utf-8"))
            
            viertel = response.xpath('//ul[@class="breadcrumb__item--current"]/preceding::a[1]/text()').get()

            if viertel:
                stadtvid = self.extractor.extractAdresse( str(viertel), 0)
                loader.add_value('stadtvid', stadtvid)
            # if stadtvid == 0 and add:
            #     stadtvid = self.extractor.extractAdresse(
            #         self.conn, str(add), 1, self.stadtid)
            #     loader.add_value('stadtvid', stadtvid)
            # else:
            #     loader.add_value('stadtvid', 0)

            loader.add_value('stadtid', self.stadtid)
            loader.add_value('anbieter', "0")
            loader.add_value('stadtname', self.stadtname)

            yield loader.load_item()

        except Exception as e:
            print("ERROR IMMOSCOUT IN PARSE ITEM:")
            traceback.print_exception(type(e), e, e.__traceback__)

    def spider_closed(self, spider):
        #self.db.setScrapedTime(self.conn, self.id)
        print("IMMOSCOUT scraped :" + 
        str(spider.crawler.stats.get_value('item_scraped_count')))
        # self.db.writeScrapStatistik(
        #     self.conn, 1, spider.crawler.stats.get_value('item_scraped_count'))
        # self.db.closeAllConnections(self.conn)
