#!/usr/bin/env python
# -*- coding: utf-8 -*-

import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from scrapy.http.request import Request
from demo_crawl.items import ImmobilieItem
from scrapy.loader import ItemLoader
from scrapy_splash import SplashRequest
from database import DataBase
import time
import logging
from scrapy.utils.log import configure_logging
from ExtractViertel import ExtractViertel
from scrapy import signals
from scrapy.http import FormRequest
import json
from scrapy.exceptions import DropItem
import traceback
from selenium import webdriver


class ImmoweltSpider(scrapy.Spider):
    custom_settings = {
        'CLOSESPIDER_ITEMCOUNT': '220',
    }

    name = 'immowelt'

    Kaufen = 0
    Haus = 0
    stadtid = 0
    stadtvid = 0
    db = None
    extractor = None
    stadtname = ""
    id = 0
    conn = None
    start_urls = []
    driver = None
    userToStadt = None
    stop = False
    progressCounter = 0
    pagesDone = 1
    paginateUrl = ''
    startUrl = ''

    def __init__(self, userToStadt, progressCounter, *args, **kwargs):
        self.userToStadt = userToStadt
        self.progressCounter = progressCounter
        self.db = DataBase()
        self.conn = self.db.create_conn()
        self.extractor = ExtractViertel()
        self.extractor.init()
        self.bereitsVorhandeneUrls = 0

        super(ImmoweltSpider, self).__init__(*args, **kwargs)

    def start_requests(self):
        try:
            self.Kaufen = self.userToStadt.get("Kaufen")
            self.Haus = self.userToStadt.get("Haus")
            self.stadtid = self.userToStadt.get("Stadtid")
            self.stadtvid = self.userToStadt.get("StadtVid")
            self.id = self.userToStadt.get('Id')
            self.stadtname = str(self.userToStadt.get('Stadt'))
            self.stadtviertel = self.userToStadt.get("StadtViertel")
            backupUrl = 'https://www.immowelt.de/liste/' + self.stadtname
            self.start_urls.clear()
            chrome_options = Options()
            # chrome_options.add_argument('--headless')
            caps = DesiredCapabilities().CHROME
            caps["pageLoadStrategy"] = "eager"  # interactive

            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')

            self.driver = webdriver.Chrome(desired_capabilities=caps,
                                           chrome_options=chrome_options)
            self.driver.set_page_load_timeout(30)
            self.driver.implicitly_wait(30)

            self.driver.get("https://www.immowelt.de/")
            self.driver.find_element_by_css_selector('#spanSearchWhat').click()
            time.sleep(1)

            if (self.Haus == 1) and (self.Kaufen == 1):
                self.driver.find_element_by_css_selector(
                    "[data-text='Haus kaufen']").click()
                backupUrl = backupUrl + '/haeuser/kaufen?sort=relevanz'

            if (self.Haus == 0) and (self.Kaufen == 1):
                self.driver.find_element_by_css_selector(
                    "[data-text='Wohnung kaufen']").click()
                backupUrl = backupUrl + '/wohnungen/kaufen?sort=relevanz'

            if (self.Haus == 1) and (self.Kaufen == 0):
                self.driver.find_element_by_css_selector(
                    "[data-text='Haus mieten']").click()
                backupUrl = backupUrl + '/haeuser/mieten?sort=relevanz'

            if (self.Haus == 0) and (self.Kaufen == 0):
                self.driver.find_element_by_css_selector(
                    "[data-text='Wohnung mieten']").click()
                backupUrl = backupUrl + '/wohnungen/mieten?sort=relevanz'

            time.sleep(2)

            self.stadtname = self.stadtname

            if self.stadtvid != 0:
                self.stadtname = str(self.stadtname) + \
                    " " + str(self.stadtviertel)
            else:
                self.stadtname = self.stadtname

            self.driver.find_element_by_css_selector(
                '#tbLocationInput').clear()
            self.driver.find_element_by_css_selector(
                '#tbLocationInput').send_keys(self.stadtname)
            time.sleep(1)
            self.driver.find_element_by_css_selector(
                '#tbLocationInput').send_keys(Keys.RETURN)
            time.sleep(5)

            if "liste" not in str(self.driver.current_url):
                print("BACKUPURL: " + backupUrl)

                yield scrapy.Request(backupUrl, callback=self.parse)

            self.paginateUrl = self.driver.current_url
            self.startUrl = self.driver.current_url
            yield scrapy.Request(self.driver.current_url, callback=self.parse)

        except Exception as e:
            traceback.print_exception(type(e), e, e.__traceback__)
            traceback.print_exception(type(e), e, e.__traceback__)
            self.driver.quit()

    def parse(self, response):
        try:
            if self.stop:
                self.crawler.engine.close_spider(
                    self, 'Zu Viele DUPLICATE URL Errors ')
                return
            next_page = response.xpath("//a[@id='nlbPlus']/@href")

            if self.pagesDone <= 5 or not next_page:
                self.driver.get(self.paginateUrl)
                self.driver.execute_script(
                    "window.scrollTo(0, document.body.scrollHeight / 2)")

                time.sleep(3)
                # self.driver.save_screenshot( "/scrap/" + str(response.url)[-10:] +".png")
                actions = ActionChains(self.driver)
                for i in range(2):
                    actions.send_keys(Keys.PAGE_UP).perform()
                    time.sleep(1)
                    actions.send_keys(Keys.PAGE_UP).perform()
                    time.sleep(1)
                elems = self.driver.find_elements_by_xpath(
                    "//a[contains(@href, 'expose')]")

                for elem in elems:
                    if elem.get_attribute("href") not in self.start_urls:
                        workUrl = elem.get_attribute("href")
                        if '?' in str(elem.get_attribute("href")):
                            workUrl = str(elem.get_attribute(
                                "href")).split('?')[0].replace('?', '')
                        self.start_urls.append(workUrl)

                    next_page = response.xpath("//a[@id='nlbPlus']/@href")
                    if next_page:
                        self.pagesDone += 1
                        self.paginateUrl = str(self.startUrl) + \
                            '&cp=' + str(self.pagesDone)

                        yield scrapy.Request(self.paginateUrl, callback=self.parse)
                        return

                    else:
                        self.pagesDone = 99
                        for url in self.start_urls:
                            if self.db.checkIfInDupUrl(self.conn, url) == False:
                                yield SplashRequest(url=url, callback=self.parse_item, meta={'progressCounter': self.progressCounter}, endpoint='render.html', headers={'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2049.0 Safari/537.36'}, dont_filter=True, args={'wait': 0.5})
                            else:
                                self.bereitsVorhandeneUrls += 1
                                if self.bereitsVorhandeneUrls >= 35:
                                    print('zu viele Duplikate, höre auf')
                                    self.stop = True
                                    self.bereitsVorhandeneUrls = 0

        except Exception as ex:
            print(ex)
            print(ex)

    def parse_item(self, response):
        try:
            respProgressCounter = response.meta['progressCounter']
            if respProgressCounter != self.progressCounter:
                print(
                    "VERSPÄTET REQUEST ERKANNT; DROPPTE ITEM SOFORT")
                raise DropItem("Verspätetet Request Behandlung")
            item = ImmobilieItem()
            loader = ItemLoader(item, selector=response, response=response)
            for info in response.xpath("//div[@class='quickfacts iw_left']"):
                loader = ItemLoader(item, selector=info, response=response)
                title = response.xpath(".//h1/text()").extract()
                loader.add_value('title', str(title).encode('utf-8'))
                loader.add_xpath(
                    'flache', ".//div[@class='hardfact '][2]/text()")
                loader.add_xpath(
                    'zimmer', ".//div[@class='hardfact rooms']/text()")
                loader.add_xpath('grundstuck', ".//div[@class='hardfact '][3]")

            # Hole Anzahl Bilder, da ITEMS öfter vorkommen als gebraucht
            for i in range(1, 8):
                try:
                    bil = 'bild%s' % (str(i))
                    xpath = None
                    content = response.xpath(
                        "//div[@class='carousel-item'][%s]/img/@src" % str(i)).get()

                    if "App_Themes" in str(content):
                        xpath = "//div[@class='carousel-item'][%s]/img/@data-src" % str(
                            i)
                    else:
                        xpath = "//div[@class='carousel-item'][%s]/img/@src" % str(
                            i)

                    loader.add_xpath(bil, xpath)
                except:
                    print("Fehler in Bildauslesen mit XPATH")

            loader.load_item()

            loader = ItemLoader(item, selector=response, response=response)
            if self.Kaufen == 0:
                loader.add_value('kaufen', '0')
                gesamtmiete = response.xpath(
                    "//div[text() = 'Warmmiete ']/../div[2]").extract_first()
                if gesamtmiete:
                    loader.add_xpath(
                        'gesamtkosten', "//div[text() = 'Warmmiete ']/../div[2]/text()")
                else:
                    loader.add_xpath(
                        'gesamtkosten', "//strong[text() = 'Warmmiete ']/../../div[@class='datacontent iw_right']/strong/text()")
                loader.add_xpath(
                    'kaltmiete', "//div[@class='datacontent iw_right']/strong/text()")
                loader.add_xpath(
                    'nebenkosten', "//div[contains(text(),'Nebenkosten')]/../div[2]/text()")
                loader.add_xpath(
                    'gesamtkosten', "//div[text() = 'Warmmiete ']/../div[2]/text()")
                loader.add_xpath(
                    'bezugsfreiab', "//div[2]/div[@class='section_content iw_right']/p/strong/text()")

            else:
                loader.add_value('kaufen', '1')
                loader.add_xpath(
                    'kaltmiete', "//div[@class='datacontent iw_right']/strong/text()")
                loader.add_xpath('provisionsfrei',
                                 "//strong[contains(text(),'provisionsfrei')]")
                loader.add_xpath(
                    'bezugsfreiab', "//div[@class='section_content iw_right']/p[2]")

            if self.Haus == 1:
                loader.add_value('haus', '1')
            else:
                loader.add_value('haus', '0')
            loader.load_item()
            add = ""
            loader = ItemLoader(item, selector=response, response=response)
            loader.add_xpath('terrasse', "//span[contains(text(),'Terrasse')]")
            loader.add_xpath(
                'keller', "//span[contains(text(),'Kelleranteil')]")
            loader.add_xpath('garten', "//span[contains(text(),'Garten')]")
            loader.add_xpath('ebk', "//span[contains(text(),'Einbauküche')]")
            add = response.xpath(
                "//div[@class='location']/span/text()").extract()
            loader.add_value('adresse', str(add).encode("utf-8"))
            loader.add_xpath(
                'aufzug', "//span[contains(text(),'Personenaufzug')]")
            loader.add_xpath('balkon', "//span[contains(text(),'Balkon')]")
            loader.add_xpath(
                'typ', "//div[2]/div[@class='section_content iw_right']/p/text()")
            loader.add_xpath(
                'barriefrei', "//span[contains(text(),'barrierefrei')]")
            loader.add_xpath(
                'haustier', "//span[contains(text(),'Haustiere erlaubt')]")
            loader.add_xpath('garage', "//span[contains(text(),'Stellplatz')]")
            url = response.xpath(
                "//input[@class='js-endlink-input']/@value").get()
            loader.add_value('url', url)
            loader.add_value('stadtid', self.stadtid)

            loader.add_value('anbieter', "1")
            loader.add_value('stadtname', self.stadtname)

            if self.stadtvid == 0 and add:
                stadtvid = self.extractor.extractAdresse(
                    self.conn, str(add), 1, self.stadtid)
                loader.add_value('stadtvid', stadtvid)
            else:
                loader.add_value('stadtvid', self.stadtvid)

            yield loader.load_item()
        except Exception as ex:
            print(ex)
            print("ERROR IN PARSEITEM IMMOWELT:" + str(ex))

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(ImmoweltSpider, cls).from_crawler(
            crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed,
                                signal=signals.spider_closed)
        return spider

    def spider_closed(self, spider):
        print("IMMOWELT scraped :" + str(spider.crawler.stats.get_value(
            'item_scraped_count')) + " IN DER STADT : " + str(self.stadtname))
        self.db.setScrapedTime(self.conn, self.id)
        self.db.writeScrapStatistik(
            self.conn, 2, spider.crawler.stats.get_value('item_scraped_count'))
        self.db.closeAllConnections(self.conn)
