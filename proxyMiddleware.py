from fake_useragent import UserAgent
import random
from scrapy.downloadermiddlewares.retry import RetryMiddleware
from scrapy.utils.response import response_status_message
import requests

import time
# A Tor IP will be reused only after 10 different IPs were used.
# tor_ip_changer = TorIpChanger(tor_password='Schranknr8!', tor_port=9051, local_http_proxy='127.0.0.1:8118')
proxies = [] # Will contain proxies [ip, port]
ua = UserAgent() # From here we generate a random user agent
# proxies_req = Request('https://www.sslproxies.org/')
# proxies_req.add_header('User-Agent', ua.random)
# proxies_doc = urlopen(proxies_req).read().decode('utf8')

# soup = BeautifulSoup(proxies_doc, 'html.parser')
# proxies_table = soup.find(id='proxylisttable')

# # Save proxies in the array
# for row in proxies_table.tbody.find_all('tr'):
#     print(row.find_all('td')[2].string)
#     tag = row.find_all('td')[2].string
#     if 'CZ' in tag or 'DE' in tag or 'GB' in tag or 'FR' in tag or 'HU' in tag or 'US' in tag:
#         proxies.append({
#         'ip':   row.find_all('td')[0].string,
#         'port': row.find_all('td')[1].string
#         })
# resp = requests.get(
#     "http://list.didsoft.com/get?email=muellerjohannes93@gmail.com&pass=sjnpji&pid=http1000&showcountry=no").text
with open('proxies.txt') as f:
    content = f.readlines()

# Show the file contents line by line.
# We added the comma to print single newlines and not double newlines.
# This is because the lines contain the newline character '\n'.
for line in content:
    proxies.append(str(line))

print('LISTE BEFÜLLT MIT ' + str(len(proxies)) + " PROXIES")
       
class ProxyMiddleware(object):
    
    _requests_count = 0
    _givetorip = 0
 

    def process_request(self, request, spider):
        # proxy_index = self.random_proxy()
        # proxy = 'http://' + proxies[proxy_index]
        # # print('proxy ist ' + str(proxy))
        # request.meta['proxy'] = proxy
        # request.headers['User-Agent'] = ua.random
        
           
    def random_proxy(self):
        return random.randint(0, len(proxies) - 1)



class TooManyRequestsRetryMiddleware(RetryMiddleware):

    def __init__(self, crawler):
        super(TooManyRequestsRetryMiddleware, self).__init__(crawler.settings)
        self.crawler = crawler

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def process_response(self, request, response, spider):
        if request.meta.get('dont_retry', False):
            return response
        elif response.status == 429:
            print('resp ist 420 bei spider ' + spider)
            self.crawler.engine.pause()
            time.sleep(60) # If the rate limit is renewed in a minute, put 60 seconds, and so on.
            self.crawler.engine.unpause()
            reason = response_status_message(response.status)
            return self._retry(request, reason, spider) or response
        elif response.status in self.retry_http_codes:
            reason = response_status_message(response.status)
            return self._retry(request, reason, spider) or response
        return response 