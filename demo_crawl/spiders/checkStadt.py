import scrapy
import os,sys,inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir) 
from datetime import datetime
from scrapy.utils.project import get_project_settings
from database import DataBase

class CheckStadtSpider(scrapy.Spider):
    name = "checkstadt"
    db = DataBase()
    stadtid = None

    def __init__(self, stadtid, *args, **kwargs):
        print('init checkspider for stadtid :', str(stadtid))
        self.stadtid = stadtid
        super(CheckStadtSpider, self).__init__(*args, **kwargs)


    def start_requests(self): 
        starturls = self.db.returnUrlsFromStadt(self.stadtid)
        for url in starturls:
            yield scrapy.Request(url=url["url"], callback=self.parse)

    def parse(self, response):
        if "scout24" in str(response.url):
            headline = response.xpath("//h3[contains(text(),'Angebot wurde deaktiviert')]")
            if  headline:
                self.db.deleteUrl(response.url)
                

        if "immonet" in str(response.url):
            headline = response.xpath("//*[contains(text(),'Objekt nicht mehr verfügbar.')]")
            if  headline:    
                self.db.deleteUrl(response.url)

        if "meinestadt" in str(response.url):
            headline = response.xpath("//*[contains(text(),'Die aufgerufene Immobilie ist bereits vergeben oder wurde zwischenzeitlich vom Anbieter entfernt')]")
            if  headline: 
                self.db.deleteUrl(response.url)

        if "sparkasse" in str(response.url):
            headline = response.xpath("//*[contains(text(),'Seite nicht gefunden')]")
            if  headline: 
                self.db.deleteUrl(response.url)

        if "wg-suche" in str(response.url):
            headline = response.xpath("//*[contains(text(),'Sieht so aus als wäre hier nichts.')]")
            if  headline:
                self.db.deleteUrl(response.url)
                
        if response.status == 404:
                self.db.deleteUrl(response.url)
        elif response.status == 301:
                self.db.deleteUrl(response.url)

