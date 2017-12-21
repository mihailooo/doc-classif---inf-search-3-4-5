# coding: utf-8

import time
import xml.etree.ElementTree as et
import sqlite3
import threading
import md5

import requests

# Минимальное число вопросов для занесения в бд.
MIN_BATCH_SIZE = 200
# Число качающих потоков.
N_THREADS = 5
# Ограничение на число обработанных документов или 0 если 
# ограничений нет. Используется при отладке. 
N_PROC_Q_LIMIT = 0

DEFAULT_DB_NAME = "db.sqlite"
# Cкрипт для создания бд.
DB_CREATION_SCRIPT = u"""
    CREATE TABLE IF NOT EXISTS question_tbl (
        question_id    TEXT PRIMARY KEY,
        question       TEXT NOT NULL,
        answer         TEXT NOT NULL,
        autor          TEXT NOT NULL 
    );
"""

ROOT_URL = "http://db.chgk.info/tour/xml"
URL_TEMPLATE = "http://db.chgk.info/tour/%(text_id)s/xml"

def process_url(url=ROOT_URL, session=None):
    """
        Скачивает и разбирает xml документ.
        Возвращает пару (список дочерних ссылок, список словарей описывающих вопрос).
        Словарь с вопросом обязательно содержит следующие ключи:
            q: вопрос
            a: ответ
            autor: автор вопроса
        url - адрес документа, если не указан, то загружается корневой xml.
        session - HTTP сессия, если не указана, то создается новая.
    """
    # Дефолтные значения для параметров.
    if url is None:
        url = URL_TEMPLATE % dict(text_id=ROOT_TEXT_ID)
    if session is None:
        session = requests.session()
    # Cкачиваем документ.    
    resp = session.get(url)
    if not resp.ok:
        raise Exception(url)
    # Строим дерево.    
    doc = et.fromstring(resp.content, parser=et.XMLParser())
    # Извлекаем ссылки на дочерние документы.
    urls = [URL_TEMPLATE % dict(text_id=elem.text.strip()) for elem in doc.findall("tour/TextId")]
    # Извлекаем вопросы.
    questions = []
    for elem in doc.findall("question"):
        questions.append(dict( q = elem.find("Question").text
                             , a = elem.find("Answer").text
                             , autor = elem.find("Authors").text ))
        if not (questions[-1]["q"] and questions[-1]["a"] and questions[-1]["autor"]):
            # Не указан автор, вопрос или ответ. Игнорируем такой вопрос.
            questions.pop()
    return urls, questions      
     
def downloader(urls, questions, errors, processed_urls):
    """
        Рабочая функция скачивающих потоков.
        Пока список urls не пуст, берет из него ссылки и обрабатывает их.
        
        urls - список ссылок по которым надо пройти. 
               Если он заканчивается, то поток завершается
        questions - список в который заносятся обнаруженые вопросы.
        errors - сюда записываются исключения всплывающие в процессе работы потока.
        processed_urls - множество, к которому добавляются обработанные ссылки.
        
        Новые обнаруженые ссылки записываются в urls. 
        
        Работа организована таким образом, что многопоточной среде, существует малая 
        вероятность того, что одна ссылка будет обработана несколько раз, но это не 
        страшно (см. consumer).
    """
    session = requests.session()
    while N_PROC_Q_LIMIT == 0 or len(processed_urls) < N_PROC_Q_LIMIT:
        try:
            url = urls.pop()
            processed_urls.add(url)
        except IndexError:
            # Ссылки закончились, завершаем работу.
            return        
        
        try:
            # Извлекаем дочерние ссылки и вопросы по ссылке.
            doc_urls, doc_questions = process_url(url, session)
            # Новые ссылки добавляем к старым.
            for u in doc_urls:
                if u not in processed_urls:
                    urls.append(u)
            # Запоминаем найденые вопросы.        
            questions.extend(doc_questions)
        except Exception as exc:
            # Что-то пошло не так, запоминаем исключение.
            errors.append(exc.message)
            # Создаем новую сессию.
            session = requests.session()
            # Ждем
            time.sleep(0.5)
             
def consumer(db_name, questions):
    """
        Рабочая функция запоминающего потока (он всегда один).
        По мере наполнения списка questions записывает вопросы в базу данных db_name.
        Если обнаруживает в конце списка None, то записывает остатки 
        и завершает работу.
    """
    # Соединяемся с базой данных.
    conn = sqlite3.connect(db_name)
    
    wait_for_more_queries = True
    while wait_for_more_queries:
        if len(questions) > MIN_BATCH_SIZE or (questions and questions[-1] is None):
            # накопилось достаточно вопросов или новых вопросов больше не будет 
            # (о чем говорит None в конце списка).
            
            # Извлекаем вопросы по одному, тк list.pop() операция атомарная, это пoзволяет 
            # не заботится о синхронизации потоков.
            batch = []  # список четверок (md5 хеш вопроса, вопрос, ответ, автор)
            while questions:
                q = questions.pop()
                if q is None:
                    wait_for_more_queries = False
                else:
                    # Вычисляем md5 хеш текста вопрос + ответ + авторы.
                    # Используем этот хеш как первичный ключ в нашей таблице.
                    batch.append(( md5.md5((q["q"] + q["a"] + q["autor"]).encode("utf8")).hexdigest()
                                 , q["q"], q["a"], q["autor"] ))
            # Записываем batch.             
            with conn:
                 conn.executemany( u""" INSERT OR IGNORE INTO question_tbl 
                                            (question_id, question, answer, autor)
                                        VALUES (?, ?, ?, ?) """, batch)
            # Выясняем сколько всего записано вопросов в бд.
            n_questions = conn.execute( u"""SELECT count(*) FROM question_tbl""").fetchone()[0]
            print "%d questions in %s." % (n_questions, db_name)
                                                
        time.sleep(0.5)
    conn.close()    
                                                
            
def main(db_name):
    # Создаем БД есили она уже существует, то ничего не изменится.
    conn = sqlite3.connect(db_name)
    conn.executescript(DB_CREATION_SCRIPT)
    conn.close()
    
    # Контейнеры для ссылок, вопросов, исключений и обработанных ссылок.
    urls = [ROOT_URL]
    questions = []
    errors = []
    processed_urls = set()
    
    # Создаем качающие потоки.
    threads = [ threading.Thread(target=downloader, args=(urls, questions, errors, processed_urls)) 
                for _ in xrange(N_THREADS)]
    for t in threads:
        t.start()
        
    # Создаем запоминающий поток.
    cons_thread = threading.Thread(target=consumer, args=(db_name, questions))
    cons_thread.start()
    
    # Дожидаемся завершения качающих потоков.
    for t in threads:
        t.join()
    
    # Cообщаем запоминающему потоку, что больше вопросов не будет.
    # Для этого записываем None в questions.
    questions.append(None)
    
    # Дожидаемся завершения запоминающего потока.
    cons_thread.join()

if __name__ == "__main__":
    main(DEFAULT_DB_NAME)