# coding: utf-8
"""
    Скачивает все новости с lenta.ru за последние TIME_PERIOD дней.
"""
import re
import md5
import time
import datetime
import lxml.etree as et
import sqlite3
import threading

import requests

TIME_PERIOD = 2*365
DEFAULT_DB_NAME = "db.sqlite"
MIN_BATCH_SIZE = 50
N_THREADS = 2

# Cкрипт для создания бд.
DB_CREATION_SCRIPT = u"""
    CREATE TABLE IF NOT EXISTS news_tbl (
        news_id    TEXT PRIMARY KEY,
        url        TEXT NOT NULL,
        category   TEXT NOT NULL,
        title      TEXT NOT NULL,
        body       TEXT NOT NULL 
    );
"""


DAY_URL_TEMPLATE = "http://m.lenta.ru/rubrics/%(ca_key)s/%(year)d/%(month)02d/%(day)02d/"
BASE_URL = "http://m.lenta.ru"
CATEGORIES = dict( russia=u"Россия"
                 , world=u"Мир"
                 , ussr=u"Бывший СССР"
                 , economics=u"Экономика"
                 , forces=u"Силовые структуры"
                 , science=u"Наука и техника"
                 , sport=u"Спорт"
                 , culture=u"Культура"
                 , media=u"Интернет и СМИ"
                 , style=u"Ценности"
                 , travel=u"Путешествия"
                 , life=u"Из жизни"
                 , motor=u"Мотор" )
NEWS_ANCHOR_XPATH = "//*[@id='root']/section[2]/div[2]/.//a[contains(@href, '/news/')]"
TOPIC_BODY_TEXT_LINES = ( "//*[@id='body']/div/div[contains(@class, 'b-topic__body')]/p/text()"
                         "|//*[@id='body']/div/div[contains(@class, 'b-topic__body')]/p/a/text()" )
# Задержка после скачивания документа.
AFTER_GET_PAUSE = .1

def get_news_body(session, url):
    # Cкачиваем документ.    
    resp = session.get(url)
    time.sleep(AFTER_GET_PAUSE)
    if not resp.ok:
        raise Exception(url)
    # Строим дерево.    
    doc = et.fromstring(resp.content, parser=et.HTMLParser(encoding="utf8"))
    return u"\n".join(doc.xpath(TOPIC_BODY_TEXT_LINES))
    
def process_day(days, news, errors):
    session = requests.session()
    while True:
        try: 
            day = days.pop()
        except IndexError:
            return
        for ca_key in CATEGORIES:    
            url = DAY_URL_TEMPLATE % dict(ca_key=ca_key, year=day.year, month=day.month, day=day.day)
            print url
            # Cкачиваем документ.    
            resp = session.get(url)
            time.sleep(AFTER_GET_PAUSE)
            if not resp.ok:
                raise Exception(url)
            # Строим дерево.    
            doc = et.fromstring(resp.content, parser=et.HTMLParser(encoding="utf8"), base_url=url)
            for a in doc.xpath(NEWS_ANCHOR_XPATH):
                news_url = url=BASE_URL + a.get("href")
                news_id = md5.md5(a.get("href").strip()).hexdigest()
                news.append(dict( url=news_url
                                , title=a.text
                                , ca_key=ca_key
                                , news_id=news_id
                                , body = get_news_body(session, news_url) ))
                if news_id[-1] in "01":
                    session = requests.session()            
                
def consumer(db_name, news):
    """
        Рабочая функция запоминающего потока (он всегда один).
        По мере наполнения списка news записывает новости в базу данных db_name.
        Если обнаруживает в конце списка None, то записывает остатки 
        и завершает работу.
    """
    # Соединяемся с базой данных.
    conn = sqlite3.connect(db_name)
    
    wait_for_more_news = True
    while wait_for_more_news:
        if len(news) > MIN_BATCH_SIZE or (news and news[-1] is None):
            # накопилось достаточно новостей или новых новостей больше не будет 
            # (о чем говорит None в конце списка).
            
            # Извлекаем новости по одному, тк list.pop() операция атомарная, это пoзволяет 
            # не заботится о синхронизации потоков.
            batch = []  # список пятерок (news_id, url, category, title, body)
            while news:
                n = news.pop()
                if n is None:
                    wait_for_more_news = False
                else:
                    batch.append(( n["news_id"], n["url"], n["ca_key"], n["title"], n["body"] ))
            # Записываем batch.             
            with conn:
                 conn.executemany( u""" INSERT OR IGNORE INTO news_tbl 
                                            (news_id, url, category, title, body)
                                        VALUES (?, ?, ?, ?, ?) """, batch)
            # Выясняем сколько всего записано новостей в бд.
            n_news = conn.execute( u"""SELECT count(*) FROM news_tbl""").fetchone()[0]
            print "%d news in %s." % (n_news, db_name)
                                                
        time.sleep(0.5)
    conn.close()
        
        
        
def main(db_name):
    # Создаем БД есили она уже существует, то ничего не изменится.
    conn = sqlite3.connect(db_name)
    conn.executescript(DB_CREATION_SCRIPT)
    conn.close()
    
    # Контейнеры для dat, новостей и ошибок.
    days = [ datetime.date.fromordinal(datetime.date.today().toordinal() - offset)
             for offset in xrange(TIME_PERIOD) ]
    news = []
    errors = []
    
    # Создаем качающие потоки.
    threads = [ threading.Thread(target=process_day, args=(days, news, errors)) 
                for _ in xrange(N_THREADS)]
    for t in threads:
        t.start()
        
    # Создаем запоминающий поток.
    cons_thread = threading.Thread(target=consumer, args=(db_name, news))
    cons_thread.start()
    
    # Дожидаемся завершения качающих потоков.
    for t in threads:
        t.join()
    
    # Cообщаем запоминающему потоку, что больше новостей не будет.
    # Для этого записываем None в news.
    news.append(None)
    
    # Дожидаемся завершения запоминающего потока.
    cons_thread.join()

if __name__ == "__main__":
    main(DEFAULT_DB_NAME)
        
        

        
            
            
            
        
    
    

