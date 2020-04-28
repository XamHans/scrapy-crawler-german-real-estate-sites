#!/usr/bin/env python
# -*- coding: utf-8 -*-
import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from scrapy.http.request import Request
from demo_crawl.items import WGItem
from scrapy.loader import ItemLoader
from scrapy_splash import SplashRequest
import re
import json
from scrapy.exceptions import DropItem
from database import DataBase
from demo_crawl import settings as my_settings
import time
from ExtractViertel import ExtractViertel
#from fake_useragent import UserAgent
from scrapy import signals
import logging

class WgsucheSpider(scrapy.Spider):

    name = 'wgsuche'
    stadtid = 0
    db = None
    conn = None
    stadt = ""
    custom_settings = { "CLOSESPIDER_ITEMCOUNT" : 150 }

    def __init__(self, stadtid, *args, **kwargs):
        self.db =  DataBase()
        self.conn = self.db.create_conn()
        self.stadtid =  stadtid
        self.extractor = ExtractViertel()
        self.extractor.init()
        print("WGSUCHE MIT STADTID:" + str(stadtid) )

        super(WgsucheSpider, self).__init__(*args, **kwargs)

    def start_requests(self): 
        ua = UserAgent()
        userAgent = ua.random
        chrome_options = Options()
        chrome_options.add_argument('user-agent=%s' %userAgent)
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--ignore-certificate-errors')
        driver = webdriver.Chrome(chrome_options=chrome_options)

        # TODO, neue returnUser funktion
        userlist = self.db.returnUsers(self.conn, self.stadtid)

        for userToStadt in userlist:         
            driver.get("https://www.wg-suche.de/")
            if userToStadt.get('Haus') is not None:
                continue

            self.stadt = userToStadt.get("Stadt")
            
            wait = WebDriverWait(driver, 60)
            input = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR , "input")))

            input.clear()
            input.send_keys(self.stadt)
            time.sleep( 2 )

            input.send_keys(Keys.RETURN)

            time.sleep( 2 )

            driver.find_element_by_xpath("//button").click()
            time.sleep( 5 )

            starturl = driver.current_url + "?page="

            urls = []
            i = 0
            j = 0
            x = 2

            while  driver.find_elements_by_xpath("//a[@class='arrows']") or j == 0: 
                if j <= 5:
                    j = j+1

                    immos = driver.find_elements(By.XPATH, "//a[contains(@href, 'angebot')]")
                    for immo in immos:
                        href = immo.get_attribute("href")
                    
                        zweiturl = re.search('\d+', href)
                        link = str ('https://api.wg-suche.de/v1_0/offer/' + zweiturl.group(0) )
                        urls.insert( i,link )
                        i = i+1

                    url = starturl + "%s"  % str(x)
                    x = x+1
                    driver.get(url)
                    time.sleep( 2 )

                else: break

            for url in urls:
                yield scrapy.Request(url, callback=self.parse)


    def parse(self, response):
        jsonresponse = json.loads(response.body_as_unicode())
        item=WGItem()
        loader = ItemLoader(item, selector=response, response=response) 

        loader.add_value("title",jsonresponse["title"])
        loader.add_value("gesamtkosten",jsonresponse["rent"])
        if "size" in jsonresponse:
            loader.add_value("flache",jsonresponse["size"])
        else:
            raise DropItem("Missing flatsize in %s" % item)    
        loader.add_value("anbieter","5")
        loader.add_value("kaution",jsonresponse["deposit"])
        if "from" in jsonresponse:
            loader.add_value("bezugsfreiab",jsonresponse["from"])
        if "membersWomanCount" in jsonresponse:
            loader.add_value("anzahlf",jsonresponse["membersWomanCount"])
        if "membersManCount" in jsonresponse:
            loader.add_value("anzahlm",jsonresponse["membersManCount"])
        if "wantedAmountFemale" in jsonresponse:
            loader.add_value("gesuchtf",jsonresponse["wantedAmountFemale"])   
        if "wantedAmountMale" in jsonresponse:
            loader.add_value("gesuchtm",jsonresponse["wantedAmountMale"])  
        if "garden" in jsonresponse:   
            loader.add_value("garten",jsonresponse["garden"])
        if "balcony" in jsonresponse:     
            loader.add_value("balkon",jsonresponse["balcony"])
        if "elevator" in jsonresponse:     
            loader.add_value("aufzug",jsonresponse["elevator"])
        if "lat" in jsonresponse:    
            loader.add_value("lat",jsonresponse["lat"])
            loader.add_value("lon",jsonresponse["lng"])
        if "barrierFree" in jsonresponse: 
            loader.add_value("barriefrei",jsonresponse["barrierFree"])
        if "street" in jsonresponse and "streetNumber" in jsonresponse:  
            loader.add_value("adresse",jsonresponse["street"] + " " + jsonresponse["streetNumber"] )
        if "street" in jsonresponse:
             loader.add_value("adresse",jsonresponse["street"])
        if "furnished" in jsonresponse:  
            loader.add_value("moebliert",jsonresponse["furnished"]  )    
        loader.add_value("url","https://www.wg-suche.de/angebot/" + str ( jsonresponse["id"] ) )
        loader.add_value("stadtid",self.stadtid)

        if "borough" in jsonresponse:
            viertel =  jsonresponse['borough']
            stadtvid = self.extractor.extractAdresse( self.conn, viertel , 2, self.stadtid)
            loader.add_value('stadtvid',stadtvid)

        
        for i in range (1,8):
            try:
                bil = 'bild%s' % str(i)
                value = jsonresponse["images"][i]["urls"]["M"]["url"] 
                loader.add_value(bil,value)
            except Exception as e:
                    traceback.print_exception(type(e), e, e.__traceback__)
        
        
        return loader.load_item()

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(WgsucheSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def spider_closed(self, spider, reason):
        print("SPIDER WGSUCHE " + self.stadt + "  CLOSED" + " REASON : " + reason )     
        print( " scraped " + str (spider.crawler.stats.get_value('item_scraped_count') ))
        self.db.writeScrapStatistik(self.conn, 4, spider.crawler.stats.get_value('item_scraped_count') )
        self.db.closeAllConnections(self.conn)
