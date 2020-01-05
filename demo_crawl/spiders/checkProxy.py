
import scrapy


class CheckProxySpider(scrapy.Spider):
    name = 'checkproxy'

    custom_settings = {
        'DUPEFILTER_CLASS': 'scrapy.dupefilters.BaseDupeFilter',
    }
    
    def start_requests(self):
        for x in range(25):
            yield scrapy.Request('http://checkip.dyndns.org/', callback=self.check_ip)
     

    def check_ip(self, response):
        pub_ip = response.xpath('//body/text()').re('\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}')[0]
        print ("My public IP is: " + pub_ip)