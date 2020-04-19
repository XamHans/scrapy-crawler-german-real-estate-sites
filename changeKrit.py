import multiprocessing 
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.action_chains import ActionChains
import threading
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
import time
import json
import re
from database import DataBase
import psutil

threadLocal = threading.local()

userToStadt = None
db = None
kritUrls = {}

def get_links():
  return ['https://www.immonet.de/', 'https://www.immobilienscout24.de/', 'https://www.meinestadt.de/deutschland/immobilien']

def get_driver():
  driver = getattr(threadLocal, 'driver', None)
  if driver is None:
    chromeOptions = webdriver.ChromeOptions()
    prefs = {"profile.managed_default_content_settings.images": 2}
    chromeOptions.add_argument('--no-sandbox')
    chromeOptions.add_argument('--headless')
    chromeOptions.add_argument('--disable-dev-shm-usage')
    # chromeOptions.add_argument('--proxy-server=http://{}'.format('173.212.249.71:8118'))
    caps = DesiredCapabilities().CHROME
    caps["pageLoadStrategy"] = "eager"  # complete
    chromeOptions.add_experimental_option("prefs", prefs)
    driver = webdriver.Chrome(
        chrome_options=chromeOptions, desired_capabilities=caps, executable_path='/usr/local/bin/chromedriver')
    setattr(threadLocal, 'driver', driver)
  return driver

def killChromies():
    for proc in psutil.process_iter():
        try:
            # check whether the process name matches
            if proc.name() == 'Google Chrome' or proc.name() == 'chromedriver' or 'chrome' in proc.name() or proc.name() == 'google-chrome':
                proc.kill()
          
        except Exception as e:
            print(e)


def doImmoScout(url, userToStadt, saver):
    driver = get_driver()
    wait = WebDriverWait(driver, 15)
    driver.get(url)
    try:
        if userToStadt.get("Haus") == 1:
            select = Select(
                driver.find_element_by_css_selector('[tabindex="3"]'))
            # select by visible text
            select.select_by_visible_text('Haus')

        if userToStadt.get("Kaufen") == 1:
            select = Select(
                driver.find_element_by_css_selector('[tabindex="1"]'))
            # select by visible text
            select.select_by_visible_text('Kaufen')

        stadtname = userToStadt.get("Stadt")

        if ',' in stadtname:
            stadtname = stadtname.split(',')[0]

        input = wait.until(EC.element_to_be_clickable(
            (By.CSS_SELECTOR, "[tabindex='2']")))
        input.send_keys(stadtname)
        time.sleep(3)
        checkIfKreis = driver.find_element_by_xpath(
            '//*[@id="ui-id-1"]/li[1]')
        if 'Kreis' in str(checkIfKreis.text):
            input.send_keys(Keys.DOWN)
            time.sleep(1)
        input.send_keys(Keys.RETURN)
        time.sleep(5)


        inputText = str(input.get_attribute('value'))
        wholeWordSearch = str(userToStadt.get("Stadt")) + r"\b"
        print(wholeWordSearch)
        if bool(re.search(r'\d', inputText)) and not bool(re.search(wholeWordSearch, inputText)):
            return
        try:
            # driver.find_element(
            #     By.XPATH, "//button[@class='oss-main-criterion oss-button button-primary one-whole']").click()
            input.send_keys(Keys.RETURN)

        except Exception as e:
            print(e)
        try:
            search = wait.until(EC.presence_of_element_located(
                (By.CSS_SELECTOR, "#searchHeader")))
        except Exception as e:
            if ' ' in userToStadt.get('Stadt'):
                userToStadt["Stadt"] = userToStadt.get('Stadt').split(" ")[0]
            doImmoScout(driver.current_url, userToStadt, saver)
            return
        print('IMMOSCOUT ' + str(driver.current_url))
        if "Suche" in driver.current_url:
            standardurl = driver.current_url
            if "-Kreis" in driver.current_url:
                url = driver.current_url.replace('-Kreis', '')
                print('IMMOSCOUT END URL : ' + str(url))
                kritUrls['immoscout'] = url
            else:
                print('IMMOSCOUT END URL : ' + str(driver.current_url))
                saver['immoscout'] = (driver.current_url); 

    except Exception as e:
        print(e)

    
def doImmoNet(url, userToStadt, saver):
    driver = get_driver()
    wait = WebDriverWait(driver, 15)
    driver.get(url)
    try:
        if userToStadt["Haus"] == 1:
            select = Select(
                driver.find_element_by_css_selector('#estate-type'))
            # select by visible text
            select.select_by_visible_text('Haus')

        if userToStadt["Kaufen"] == 1:
            kaufBtn = driver.find_element_by_xpath(
                "//button[@class='btn btn-80 col-xs-4']")
            kaufBtn.click()
            stadtname = userToStadt["Stadt"]
            
        stadtname = userToStadt["Stadt"]
        if ',' in stadtname:
                stadtname = stadtname.split(',')[0]
                stadtname = stadtname
        if ' ' in stadtname:
                stadtname = stadtname.split(' ')[0]
                
        input = wait.until(EC.presence_of_element_located(
            (By.CSS_SELECTOR, "#location")))
        
        input.send_keys(stadtname)
        time.sleep(1)
        input.send_keys(Keys.RETURN)
        try:
            searchBtn = driver.find_element_by_css_selector(
                        "#btn-int-find-immoobjects")
            searchBtn.click()
        except Exception as e:
            pass
        try:
            search = wait.until(EC.presence_of_element_located(
                (By.CSS_SELECTOR, "#searchfilter")))
        except Exception as e:
            if ' ' in userToStadt.get('Stadt'):
                userToStadt["Stadt"] = userToStadt.get('Stadt').split(" ")[0]
            doImmoNet(driver.current_url, userToStadt, saver)
            return       
        url = driver.current_url
        print('IMMONET END URL : ' + str(url))
        saver['immonet'] = (url); 

    except Exception as e:
        print(e)


def doMeineStadt(url, userToStadt, saver):
    driver = get_driver()
    driver.get(url)
    Kaufen = userToStadt.get("Kaufen")
    Haus = userToStadt.get("Haus")
    stadtid = userToStadt.get("Stadtid")
    stadtname = userToStadt.get("Stadt")
    wait = WebDriverWait(driver, 15)
    try:
        if ',' in stadtname:
            stadtname = stadtname.split(',')[0]
        time.sleep(6)
        try:
            input = wait.until(EC.presence_of_element_located(
                (By.CSS_SELECTOR, "#js-relocationInputId--16")))
            input = driver.find_element(By.CSS_SELECTOR, '#js-relocationInputId--16')

            time.sleep(3)
            input.clear()
            time.sleep(2)
            input.send_keys(stadtname)
            time.sleep(2)

            input.send_keys(Keys.RETURN)
            time.sleep(2)
            input.send_keys(Keys.RETURN)

        except Exception as e:
            print(e)
            pass
       
        if 'deutschland' in str(driver.current_url):
            autocomplete = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "#js-relocationSubmitBtnId--18")))
            driver.execute_script("arguments[0].scrollIntoView();", autocomplete)
            try:
                driver.find_element(By.CSS_SELECTOR, '#js-relocationSubmitBtnId--18').click()
            except Exception as e:
                print(e)

        advancedUrl = ''
        if Kaufen == 1 and Haus == 1:
            advancedUrl = '/haus-kaufen'
        elif Kaufen == 1 and Haus == 0:
            advancedUrl = '/wohnung-kaufen'
        elif Kaufen == 0 and Haus == 1:
            advancedUrl = '/haus-mieten'

    
        time.sleep(5)
       

        print('MEINE STADT CHECKE URL')
        if 'deutschland' in str(driver.current_url):
            doMeineStadt("https://www.meinestadt.de/deutschland/immobilien", userToStadt, saver)
        standardurl = driver.current_url
        if advancedUrl:
            standardurl = standardurl + advancedUrl
    
        if Haus == 0 and Kaufen == 1:
            standardurl = standardurl + '?service=immoweltAjax&esr=1&etype=1&pageSize=100'
        elif Haus == 1 and Kaufen == 1:  
            standardurl = standardurl + '?service=immoweltAjax&esr=1&etype=2&pageSize=100'
        elif Haus == 1 and Kaufen == 0:  
            standardurl = standardurl + '?service=immoweltAjax&esr=2&etype=2&pageSize=100'
        else:
            standardurl = standardurl + '?service=immoweltAjax&pageSize=100'
              
        print("MEINESTADT END URL " + str(standardurl))
        saver['meinestadt'] = standardurl; 
    except Exception as e:
        print('MEINE STADT EXCP')
        print(e)
   
    
if __name__ == '__main__':
    db = DataBase()
    conn = db.create_conn()
    stadtList = db.returnChangedKritids(conn)
    manager = multiprocessing.Manager()
    return_dict = manager.dict()
    for item in range(len(stadtList)):
        userToStadt = stadtList.pop()
        kritUrls['kritid'] = userToStadt.get('Kritid')
        kritUrls['haus'] = userToStadt.get('Haus')
        kritUrls['kaufen'] = userToStadt.get('Kaufen')
        kritUrls['stadtid'] = userToStadt.get('Stadtid')
        kritUrls['stadtname'] = userToStadt.get('Stadt')
        if ' ' in str(userToStadt.get('Stadt')):
            userToStadt['Stadt'] = userToStadt.get('Stadt').split(' ')[0]
        print('process item ' + str(userToStadt))
        p1 = multiprocessing.Process(target=doMeineStadt, args=("https://www.meinestadt.de/deutschland/immobilien",userToStadt,return_dict )) 
        p2 = multiprocessing.Process(target=doImmoScout, args=("https://www.immobilienscout24.de/",userToStadt,return_dict )) 
        p3 = multiprocessing.Process(target=doImmoNet, args=('https://www.immonet.de/',userToStadt,return_dict )) 
    
        p1.start() 
        p2.start() 
        p3.start()
        p1.join() 
        p2.join() 
        p3.join()
       
        for item in return_dict:
            kritUrls[item] = return_dict[item]
        print('ALLE FERTIG Kriturls ist ' + str(kritUrls) )

        db.insertUrlsForKrit(kritUrls)
        db.setChangedToKrit(conn, userToStadt.get('Kritid'))
        kritUrls.clear()
        return_dict.clear()
        killChromies()


            