import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
import io
import requests
import pandas as pd
import sqlalchemy as sa
import telebot
from datetime import datetime
import time
import random


class TelegramBot:
    def __init__(self, token: str, chat_id: int):
        self.bot = telebot.TeleBot(token)
        self.chat_id = chat_id
    
    def send_message(self, **kwargs):
        
        text = f'''{kwargs['title']}
{kwargs['price']}
{kwargs['addres']}
{kwargs['underground']}
{kwargs['link']}

{kwargs['description']}
{kwargs['pub_ts']}
'''
        # отправить фото с описанием
        if kwargs['photo_bytes']:
            self.bot.send_photo(self.chat_id, kwargs['photo_bytes'], caption=text)
        else:
            self.bot.send_message(self.chat_id, text)


class DataBase:
    def __init__(self, user=None, password=None, host=None, port=None, db=None):
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.db = db
        self.engine = sa.create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}')

    def send_to_db(self, df: pd.DataFrame):
        df.to_sql('my_table', self.engine, if_exists='append', index=False, schema='my_schema')
    
    def get_from_db(self, query: str):
        df = pd.read_sql(query, self.engine)
        return df
    

class AvitoParse:
    def __init__(self, url: str, db: DataBase, bot: TelegramBot, count=100, version_main=None):
        self.url = url
        self.count = count
        self.version_main = version_main
        self.db = db
        self.bot = bot
    
    def __set_up(self):
        self.driver = uc.Chrome(version_main=self.version_main)
    
    def __open_url(self):
        self.driver.get(self.url)
    
    def __paginator(self):
        while self.driver.find_elements(By.CSS_SELECTOR, '[data-marker="pagination-button/nextPage"]') and self.count > 0:
            self.driver.find_element(By.CSS_SELECTOR, '[data-marker="pagination-button/nextPage"]').click()
            self.count -= 1

    def __get_data(self, item) -> dict:
        link = item.find_element(By.CSS_SELECTOR, '[data-marker="item-title"]').get_attribute('href')
        driver_page = uc.Chrome(version_main=self.version_main)
        try:
            item_page = driver_page.get(link)
            t = random.randint(5, 10)
            print(t)
            time.sleep(t)
            item_id = driver_page.find_element(By.CSS_SELECTOR, '[data-marker="item-view/item-id"]').text.replace('№ ', '')
            title = driver_page.find_element(By.CSS_SELECTOR, '[data-marker="item-view/title-info"]').text
            description = driver_page.find_element(By.CSS_SELECTOR, '[data-marker="item-view/item-description"]').find_element(By.CSS_SELECTOR, 'p').text
            price = driver_page.find_element(By.CSS_SELECTOR, '[data-marker="item-view/item-price"]').get_attribute('content')
            item_addres_block = driver_page.find_element(By.CSS_SELECTOR, '[itemprop="address"]')
            # найти спан
            addres = item_addres_block.find_element(By.CSS_SELECTOR, 'span').text
            # найти div внутри span внутри первый span внутри второй спан
            underground = item_addres_block.find_element(By.CSS_SELECTOR, 'div') \
                .find_element(By.CSS_SELECTOR, 'span') \
                .find_elements(By.CSS_SELECTOR, 'span')[0] \
                .find_elements(By.CSS_SELECTOR, 'span')[1].text
            # pub_ts - timestamp реального времени
            pub_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            src = driver_page.find_element(By.CSS_SELECTOR, '[data-marker="image-frame/image-wrapper"]').find_element(By.CSS_SELECTOR, 'img').get_attribute('src')
            if src:
                response = requests.get(src)
                if response.status_code == 200:
                    photo_bytes = io.BytesIO(response.content)
            else:
                photo_bytes = None
            driver_page.quit()
            return {
                'id': item_id,
                'title': title,
                'description': description,
                'price': price,
                'addres': addres,
                'underground': underground,
                'pub_ts': pub_ts,
                'photo_bytes': photo_bytes,
                'link': link
            }
        except Exception as e:
            self.bot.bot.send_message(self.bot.chat_id, f'Ошибка, проверить логи')
            with open('log.txt', 'a') as f:
                f.write(f'{str(e)}\n')
            driver_page.quit()
            return None
    
    def parse(self):
        self.__set_up()
        self.__open_url()
        print('Open url')
        # сон от 1 до 10 секунд
        t = random.randint(5, 10)
        print(t)
        time.sleep(t)

        items = self.driver.find_elements(By.CSS_SELECTOR, '[data-marker="item"]')
        for item in items:
            try:
                print(item)
                item_id = int(item.get_attribute('data-item-id'))
                query = 'select distinct id from my_schema.my_table'
                df = self.db.get_from_db(query)
                if df is None:
                    continue
                print(item_id)
                # проверить, что data[id] нет в базе
                if item_id not in df['id'].values:
                    data = self.__get_data(item)              
                    self.bot.send_message(**data)
                    df = pd.DataFrame({
                        'id': [data['id']],
                        'title': [data['title']],
                        'description': [data['description']],
                        'price': [data['price']],
                        'addres': [data['addres']],
                        'underground': [data['underground']],
                        'pub_ts': [data['pub_ts']]
                    })
                    self.db.send_to_db(df)
                else:
                    print('Такой id уже есть в базе')
                    break
            except Exception as e:
                self.bot.bot.send_message(self.bot.chat_id, f'Ошибка, проверить логи')
                with open('log.txt', 'a') as f:
                    f.write(f'{str(e)}\n')
                continue
        self.driver.quit()


if __name__ == '__main__':
    token = ''
    chat_id = ''
    bot = TelegramBot(token, chat_id)
    url = ''
    db = DataBase()
    ap = AvitoParse(url, db, bot, version_main=126)
    while True:
        try:
            ap.parse()
            time.sleep(60*2)
        except Exception as e:
            bot.bot.send_message(chat_id, f'Ошибка, проверить логи')
            with open('log.txt', 'a') as f:
                f.write(f'{str(e)}\n')