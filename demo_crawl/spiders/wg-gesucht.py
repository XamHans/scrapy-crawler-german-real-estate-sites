from selenium import webdriver
driver = webdriver.PhantomJS()
driver.set_window_size(1120, 550)
driver.get("https://www.wg-gesucht.de/wg-zimmer-in-Regensburg-Galgenberg.7972976.html")
print(driver.current_url)
driver.quit()