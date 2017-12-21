# coding: utf-8
"""
    Строит и применяет классификатор новостей.
"""
from __future__ import division

import sqlite3

import numpy as np

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import RidgeClassifier
from sklearn.pipeline import make_pipeline



def db_raw_text_iter(conn, order_by="ORDER BY news_id"):
    """
        Итератор проходит таблице новостей news_tbl (см. news_collector.py) 
        в порядке order_by. И влзвращает текст состоящий из заголовка новости 
        и ее содержания.
        conn - соединение с базой.
        order_by - порядок сортировки нвовстей.
    """
    sql = """
        SELECT title, body FROM news_tbl %s;
    """ % order_by
    curr = conn.execute(sql)
    while True:
        rows = curr.fetchmany()
        if not rows: break
        for row in rows:
            yield u"\n".join(row)
    
def db_categories(conn, order_by="ORDER BY news_id"):
    """
        Возвращает категории новостей из таблицы news_tbl (см. news_collector.py) 
        в порядке order_by.
        conn - соединение с базой.
        order_by - порядок сортировки нвовстей.
    """
    sql = "SELECT category FROM news_tbl %s;" % order_by
    return np.asarray(zip(*conn.execute(sql).fetchall())[0])
    
    
if __name__ == "__main__":
    # Используем параметры из классификатора подобраные в другой задаче классификации новостей.
    # В прошлой задаче, параметры в довольно широких пределах не оказывали существенного 
    # влияния на точность классификации.
    import cPickle as pickle
    
    # Соединяемся с дб.
    conn = sqlite3.connect("db.sqlite")
    
    # Считываем данные из базы.
    groups = db_categories(conn)
    text_iter = db_raw_text_iter(conn)
    
    # Создаем и тренеруем классификатор.
    classif = make_pipeline(TfidfVectorizer(sublinear_tf=True, max_df=0.5), RidgeClassifier(alpha=0.5))
    classif.fit(text_iter, groups)
    
    # Сохраняем полученый классификатор.
    with open("news_category_classif.pickle", "wb") as f:
        pickle.dump(classif, f, -1)