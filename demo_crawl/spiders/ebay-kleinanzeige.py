#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from scrapy.http.request import Request
from demo_crawl.items import ImmobilieItem
from scrapy.loader import ItemLoader
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium import webdriver
# from importlib import reload
import time
from scrapy import signals
from selenium.webdriver.common.action_chains import ActionChains
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
    }

    name = 'ebaykleinanzeige'

    Kaufen = 0
    Haus = 0
    stadtid = 0
    x = 0
    stadtname = ""
    standardurl = ""
    driver = None
    id = 0
    conn = None
    stop = False
    userToStadt = None
    extractor = None
    foundImmos = 0

    def __init__(self, *args, **kwargs):
        self.db = DataBase()
        self.conn = self.db.create_conn()
        self.extractor = ExtractViertel()
        self.extractor.init()
        super(ImmonetSpider, self).__init__(*args, **kwargs)

    def start_requests(self):

        self.Kaufen = 0
        self.Haus = 0  #Haus kann Null None sein, check vorher
        self.stadtid = 345
        self.stadtvid = 0
        self.stadtname = 'Regensburg'
        try:
            chrome_options = Options()
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--disable-dev-shm-usage')
            driver = webdriver.Chrome(
                chrome_options=chrome_options, executable_path='/usr/local/bin/chromedriver')
            wait = WebDriverWait(driver, 60)
            if self.Haus == 1 and self.Kaufen == 1:
                driver.get("https://www.ebay-kleinanzeigen.de/s-haus-kaufen/c203")
            elif self.Haus == 1 and self.Kaufen == 0:
                driver.get("https://www.ebay-kleinanzeigen.de/s-haus-mieten/c205")
            elif self.Haus == 0 and self.Kaufen == 1:
                driver.get("https://www.ebay-kleinanzeigen.de/s-wohnung-kaufen/c196")
            elif self.Haus == 0 and self.Kaufen == 0:
                driver.get("https://www.ebay-kleinanzeigen.de/s-wohnung-mieten/c203")
        except Exception as ex:
            print(ex)
            raise CloseSpider("IMMONET COULD NOT INIT CHROME " + str(ex))


        if ',' in self.stadtname:
            self.stadtname = stadtname.split(',')[0]
       
        input = wait.until(EC.presence_of_element_located(
            (By.CSS_SELECTOR, "#site-search-area")))
      
        input.send_keys(self.stadtname)
        time.sleep(1)
      
        input.send_keys(Keys.RETURN)
 
        time.sleep(5)

        yield scrapy.Request(driver.current_url, callback=self.parse, meta={"stadtid": self.stadtid})

        driver.quit()

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

        if response.status == 410:
            print("HALLO 404")
            yield scrapy.Request(self.standardurl, callback=self.parse, meta={"stadtid": self.stadtid})

    
        immos = response.xpath(
            "//h2//a[contains(@href,'anzeige')]").extract()
        for i in immos:
            try:
                text = i.split('>')[1].lower()
                if 'gesucht' in text or 'suche eine' in text:
                    print(text)
                # if self.db.checkIfInDupUrl(self.conn,  url) == False:
                #     yield scrapy.Request(url=url, callback=self.parse_item, dont_filter=True, meta={"stadtid": stadtid, "ortsviertel": ortsViertel})

            except Exception as e:
                print("ERROR IN IMMONET PARSE:" )
                traceback.print_exception(type(e), e, e.__traceback__)

        next_page = response.xpath(
            "//a[contains(@class,'col-sm-3 col-xs-1 pull-right text-right')]/@href")
        if next_page:
            url = response.urljoin(next_page[0].extract())
            yield scrapy.Request(url, self.parse, meta={"stadtid": self.stadtid})

    def parse_item(self, response):
        try:
            item = ImmobilieItem()
            loader = ItemLoader(item, selector=response, response=response)

            loader.add_xpath(
                'title', "//h1[@id='expose-headline']/text()")
            typ = response.xpath(
                "//h2[@id='sub-headline-expose']/text()").get()
            loader.add_value('typ', str(typ).split(' ')[0])
            loader.add_xpath(
                'bezugsfreiab', "//div[@id='deliveryValue']/text()")
            loader.add_xpath(
                'haustier', "//span[contains(text(),'Haustiere')]/text()")
            garage = response.xpath(
                "//span[contains(text(),'Garage')]/text()")
            tgarage = response.xpath(
                "//span[contains(text(),'Tiefgarage')]/text()")
            stellplatz = response.xpath(
                "//span[contains(text(),'Stellplatz')]/text()")
            if garage or tgarage or stellplatz:
                loader.add_xpath('garage', "1")
            loader.add_value(
                'url', response.url)

            for i in range(1, 8):
                try:
                    bil = 'bild%s' % (str(i))
                    xpath = '//div[@class="fotorama "]/div[%s]/@data-full' % (
                        str(i))
                    bildUrl = response.xpath(xpath).get()
                    loader.add_xpath(bil, xpath)
                except Exception as e:
                    traceback.print_exception(type(e), e, e.__traceback__)
                    print("Fehler in Bild xpath Auslesen")

            loader.add_xpath(
                'zimmer', "//div[@id='equipmentid_1']/text()")

            flache = str(response.xpath(
                "//div[@id='areaid_1']/text()").get()).strip().split('.')[0]
            loader.add_value('flache', flache)

            if self.Kaufen == 0:
                loader.add_value('kaufen', '0')
                kaltm = response.xpath("//div[@id='priceid_2']/text()").get()
                nebenk = response.xpath( "//div[@id='priceid_20']/text()").get()
                heizk = response.xpath("//div[@id='priceid_5']/text()").get()
                gesamtk = response.xpath("//div[@id='priceid_4']/text()").get()

                loader.add_value('kaltmiete', kaltm)
                loader.add_value('nebenkosten', nebenk)
                if not gesamtk:
                	gesamtk = kaltm
                	loader.add_value('gesamtkosten', gesamtk)

            else:
                loader.add_value('kaufen', '1')
                kaltm = str(response.xpath(
                    "//div[@id='priceid_1']/text()").get()).strip().split('.')[0]
                loader.add_value('kaltmiete', kaltm)

                prov = response.xpath("//div[@id='courtageValue']/text()")
                if 'provisionsfrei' in prov:
                    loader.add_value('provisionsfrei', 1)
                else:
                    loader.add_value('provisionsfrei', 0)

            if self.Haus == 1:
                loader.add_value('haus', '1')
                loader.add_xpath(
                    'grundstuck', "//div[@id='areaid_3']/text()")
            else:
                loader.add_value('haus', '0')

            add = ""

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
                    self.conn, str(ortsviertel), 2, self.stadtid)
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
        self.db.writeScrapStatistik(
            self.conn, 1, spider.crawler.stats.get_value('item_scraped_count'))
        self.db.closeAllConnections(self.conn)
