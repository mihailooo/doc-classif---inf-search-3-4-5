# coding: utf-8
"""
    Индексирует в Elasticsearch скаченые в db.sqlite новости.
"""
import re
import sqlite3

INDEX_NAME = 'lenta-news-index'

from elasticsearch import Elasticsearch


if __name__ == "__main__":
    
    es = Elasticsearch()
    
    # Проверяем что elasticsearch запущен.
    es = Elasticsearch()
    if not es.ping():
        print "Start elasticsearch server befor."
        raw_input("Press enter ...")
        import sys
        sys.exit()

    
    # Если индекс уже существует, то удаляем его.
    if es.indices.exists([INDEX_NAME]):
        es.indices.delete(INDEX_NAME)
    
    # Создаем индекс.
    es.indices.create(index=INDEX_NAME)

    conn = sqlite3.connect("db.sqlite")
    
    curr = conn.execute("SELECT url, category, title, body FROM news_tbl")
    
    while True:
        rows = curr.fetchmany()
        if not rows:
            break
        for url, category, title, body in rows:
            es.index(index=INDEX_NAME, doc_type="news", body=dict(url=url, category=category, title=title, body=body))
