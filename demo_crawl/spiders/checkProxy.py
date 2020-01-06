
import scrapy
import logging


class CheckProxySpider(scrapy.Spider):
    name = 'checkproxy'

    custom_settings = {
        'DUPEFILTER_CLASS': 'scrapy.dupefilters.BaseDupeFilter',
    }
    
    def start_requests(self):
        yield scrapy.Request('http://checkip.dyndns.org/', callback=self.check_ip)
     

    def check_ip(self, response):
        pub_ip = response.xpath('//body/text()').re('\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}')[0]
        logging.error("My public IP is: " + pub_ip)
