import csv
import os
import mysql.connector
import random

from seleniumwire import webdriver 
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import time
from selenium.webdriver.chrome.service import Service
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy import text
from concurrent.futures import ThreadPoolExecutor
from settings import proxies, PROXY_PROTOCOL, PROXY_USERNAME, PROXY_PASSWORD, db_amz_shenzhen_2_108
from selenium.common.exceptions import NoSuchElementException

current_path = os.path.dirname(os.path.realpath(__file__))

db_amz_shenzhen_2_108_engine = create_engine(f'mysql+pymysql://{db_amz_shenzhen_2_108["username"]}:{db_amz_shenzhen_2_108["password"]}@{db_amz_shenzhen_2_108["host"]}:{db_amz_shenzhen_2_108["port"]}/{db_amz_shenzhen_2_108["dbname"]}'
                                    ,pool_size=2, 
                                    max_overflow=10, 
                                    pool_timeout=10, 
                                    pool_recycle=-1)
Session_108 = sessionmaker(bind=db_amz_shenzhen_2_108_engine)
download_path = r"C:\Users\tomdu\Desktop\code\python\multi_threading\google_trends_scrape20_108\google_trends_data"
file_path = download_path + r"\multiTimeline.csv"
def get_asin_keyword():
    try:
        Session_108 = sessionmaker(bind=db_amz_shenzhen_2_108_engine)
        session_108 = Session_108()
        sql = f"SELECT asin, keywords FROM db_amz_new_release.total;"
        keyword_res = session_108.execute(text(sql))
        keyword_ls = keyword_res.all()
        return keyword_ls
    except Exception as e:
        print(f"get_batch--exception", e)           
        # roll back
        session_108.rollback()
    finally:
        session_108.close()

def get_data_from_csv():
    data = []  # List to store the CSV data

    # Open the CSV file
    with open(file_path, 'r') as file:
        # Create a CSV reader
        reader = csv.reader(file)

        # Skip the first line (header)
        next(reader)
        next(reader)
        next(reader)

        # Read the CSV data line by line
        for row in reader:
            # Extract the date and value
            value = int(row[1])
            # Add the dictionary to the data list
            data.append(value)
    return data

# convert data to string 
def data_arr_to_str(data):
    data_str = ''
    for i in range(0, len(data)):
        data_str += str(data[i])
        if i != len(data) - 1:
            data_str += ','
    return data_str

def store_data_sql(single_asin_data_str_tp, table_name):
    try:
        connection = mysql.connector.connect(
            host="192.168.2.108",
            database="visualization",
            user="tom",
            password="tom_1017",
        )
        if connection.is_connected():
            cursor = connection.cursor()
            sql = f"""INSERT INTO {table_name} (asin,google_trends_series_day) VALUES (%s, %s) 
            ON DUPLICATE KEY UPDATE google_trends_series_day = VALUES(google_trends_series_day);"""
            cursor.execute(sql, single_asin_data_str_tp)
            connection.commit()
    except Exception as e:
        connection.rollback()
        print(f"{single_asin_data_str_tp[0]}--exception", e)
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()


class Proxy:
    def __init__(self):
        self.protocol = PROXY_PROTOCOL
        self.username = PROXY_USERNAME
        self.password = PROXY_PASSWORD
        self.proxy_list = proxies
 
    def url(self):
        proxy = random.choice(proxies)
        url = f"{self.protocol}://{self.username}:{self.password}@{proxy}"
        return url
    
    def get_options(self):
        proxy_url = self.url()
        proxies = {
            "http": proxy_url,
            "https": proxy_url,
        }
        options = {
            'proxy': proxies
        }
        return options
        
# relocate the download folder
class GoogleTrendsScraper:
    def __init__(self,start_date="7/1/2020",end_date="7/11/2020",region="Worldwide",query="Python programming"): 
        self.start_date = start_date
        self.end_date = end_date
        self.region = region
        self.query = query
        self.chrome_arguments = [
            "--window-size=1920,1080",
            "--disable-extensions",
            # "--headless",  # Uncomment this line if you want to run Chrome in headless mode
            "--disable-gpu",
            "--disable-software-rasterizer",
            "--no-sandbox",
            "--ignore-certificate-errors",
            "--allow-running-insecure-content",
            "blink-settings=imagesEnabled=false"
        ]
        self.chromedriver_path = "chromedriver.exe"

    def start_chrome(self,):
        chrome_options = webdriver.ChromeOptions()
        for arg in self.chrome_arguments:
            chrome_options.add_argument(arg)
        prefs = {'download.default_directory' : download_path}
        chrome_options.add_experimental_option('prefs', prefs)
        service = Service(self.chromedriver_path)
        options = Proxy().get_options()
        print(f"proxy_url--{options}")
        try:
            driver = webdriver.Chrome(service=service,options=chrome_options,seleniumwire_options=options)
            return driver
        except Exception as e:
            print(f"In the process of initialing a chromedriver, {e} happened......")
            return None
    
    def go_to_google_trends(self):
        driver = self.start_chrome()
        if driver is None:
            return None
        else:
            try:
                driver.get("https://trends.google.com/trends/?geo=US")
                time.sleep(1)
            except Exception as e:
                print(f"In the process of going to the main webpage, {e} happened......")
                return None 
            try:
                search_box = driver.find_element(By.XPATH,'//*[@id="i7"]')
                time.sleep(1)
                search_box.clear()
                search_box.send_keys(self.query)
                search_box.send_keys(Keys.RETURN)
                time.sleep(1)
            except NoSuchElementException:
                print("The xpath of the search_box has changed, have a instant check please......")
                return None
            except Exception as e:
                print(f"In the process of locate the search_box, {e} happened......")
                return None                
        return driver
    
    def change_query(self, driver:webdriver.Chrome, query):
        self.query = query
        try:
            query_input = driver.find_element(By.XPATH,'//*[@id="input-24"]')  
            query_input.click()
            time.sleep(1)
            query_input.send_keys(Keys.BACKSPACE)
            time.sleep(1)
            query_input.send_keys(self.query)
            query_input.send_keys(Keys.RETURN) 
            time.sleep(1)
        except NoSuchElementException:
            print("The xpath of the query_input has changed, have a instant check please......")
            return None
        time.sleep(1)
        return driver

    def thirty_days_time_range(self, driver:webdriver.Chrome):
        try:
            time_range = driver.find_element(By.XPATH,'//*[@id="compare-pickers-wrapper"]/div/custom-date-picker')
            time_range.click()
            time.sleep(1)
        except NoSuchElementException:
            print("The xpath of the time_range has changed, have a instant check please......")
            return None    
        try:
            thirty_days = driver.find_element(By.XPATH,'//*[@id="select_option_17"]')
            time.sleep(1)
            thirty_days.click()
            time.sleep(1)
        except NoSuchElementException:
            print("The xpath of the thirty_days button has changed, have a instant check please......")
            return None
        return driver
    
    def customize_time_range(self, driver:webdriver.Chrome):
        try:
            time.sleep(1)
            time_range = driver.find_element(By.XPATH,'//*[@id="compare-pickers-wrapper"]/div/custom-date-picker')
            time_range.click()
            time.sleep(1)
        except NoSuchElementException:
            print("The xpath of the time_range button has changed, have a instant check please......")
            return None
        try:
            custom_time_range = driver.find_element(By.XPATH,'//*[@id="select_option_22"]') 
            custom_time_range.click()
            time.sleep(1)
        except NoSuchElementException:
            print("The xpath of the time_range button has changed, have a instant check please......")
            return None
        try: 
            from_date = driver.find_element(By.XPATH,'/html/body/div[3]/div[4]/md-dialog/md-tabs/md-tabs-content-wrapper/md-tab-content[1]/div/md-content/form/div[1]/md-datepicker/div[1]/input')
            from_date.click()
            from_date.clear()
            from_date.send_keys(self.start_date)
            time.sleep(1)
        except NoSuchElementException:
            print("The xpath of the time_range button has changed, have a instant check please......")
            return None
        try:       
            to_date = driver.find_element(By.XPATH,'/html/body/div[3]/div[4]/md-dialog/md-tabs/md-tabs-content-wrapper/md-tab-content[1]/div/md-content/form/div[2]/md-datepicker/div[1]/input')
            to_date.click()
            to_date.clear()
            to_date.send_keys(self.end_date)
            time.sleep(1)
        except NoSuchElementException:
            print("The xpath of the time_range button has changed, have a instant check please......")
            return None   
        try:        
            ok_button = driver.find_element(By.XPATH,'/html/body/div[3]/div[4]/md-dialog/md-dialog-actions/button[2]')
            ok_button.click()
            time.sleep(1)
        except NoSuchElementException:
            print("The xpath of the time_range button has changed, have a instant check please......")
            return None 
        return driver
    
    def customize_region(self, driver:webdriver.Chrome):
        try:
            time.sleep(1)
            region = driver.find_element(By.XPATH,'//*[@id="compare-pickers-wrapper"]/div/hierarchy-picker[1]')
            region.click()
            time.sleep(1)
        except NoSuchElementException:
            print("The xpath of the region button has changed, have a instant check please......")
            return None 
        try:
            input_region = driver.find_element(By.XPATH,'//*[@id="input-8"]')
            input_region.send_keys(self.region)
            input_region.send_keys(Keys.ARROW_DOWN)
            input_region.send_keys(Keys.RETURN)
        except NoSuchElementException:
            print("The xpath of the input_region button has changed, have a instant check please......")
            return None 
        time.sleep(1)
        return driver
    
    def download_csv(self, driver:webdriver.Chrome):
        try:
            time.sleep(3)
            download_button = driver.find_element(By.CSS_SELECTOR,".widget-actions-item.export")
            download_button.click() 
            time.sleep(2)
        except NoSuchElementException:
            print("The xpath of the download_button has changed, have a instant check please......")
            return None 
        # self.driver.quit()
        return driver
    
    def run(self, query):
        try:
            driver = self.go_to_google_trends()
            if driver is None:
                return None

            driver = self.change_query(query, driver)
            driver = self.thirty_days_time_range(driver)
            driver = self.download_csv(driver)

            return True
        except (TypeError, AttributeError):
            return None


def multiple_thread_scrape(scraper,queries):
    for query in queries:
        scraper.run(query)
        print(f"{query} is done")

def create_nums_scrapers(num):
    scrapers = []
    for i in range(num):
        scraper = GoogleTrendsScraper(start_date="7/1/2020",end_date="7/11/2020",region="Worldwide",query="Python programming")
        scrapers.append(scraper)
    return scrapers

def thread_task(batch_id):
    start_time = time.time()
    scraper1 = GoogleTrendsScraper()
    scraper1.go_to_google_trends()
    scraper1.thirty_days_time_range()
    size = 20
    for i in range(batch_id*size, (batch_id+1)*size):
        try:
            scraper1.change_query(keyword_arr[i])
            scraper1.download_csv()
            # multiple_thread_scrape(scraper1,queries)
            print(f"{i}--- {time.time() - start_time} seconds ---")
            with open('asin_success.txt', 'a') as f:
                f.write(f"{i}--{asin_arr[i]}--\n")

        except Exception as e:
            print(f"exception", e)
            print(f"exception--{time.time() - start_time} seconds ---")
            # write exception to a file asin_exec.txt
            with open('asin_exec.txt', 'a') as f:
                f.write(f"{i}--{asin_arr[i]}--\n")
            continue
    
    # close the browser
    scraper1.driver.quit()
    scraper1.service.stop()
    # destroy the scraper
    del scraper1

if __name__ == "__main__":
    keyword_ls = get_asin_keyword()
    keyword_arr = []
    asin_arr = []
    for i in range(0, len(keyword_ls)):
        if keyword_ls[i] == None:
            continue
        if keyword_ls[i][1] == None:
            continue
        string = keyword_ls[i][1]
        string = string.replace('"', '').replace('[', '').replace(']', '')
        string_list = string.split(', ')
        keyword_arr.append(string_list[0])
        keyword_set = set(keyword_arr)
        keyword_arr = list(keyword_set)        
        asin_arr.append(keyword_ls[i][0]) 

    # use concurrentthreadpool to run thread_task
    # thread_task(0)
    start_list = [i for i in range(50,55)]
    with ThreadPoolExecutor(max_workers=1) as executor:
        futures = [executor.submit(thread_task, batch_id) for batch_id in start_list]