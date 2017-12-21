# coding: utf-8
import sqlite3
import cPickle as pickle

from elasticsearch import Elasticsearch
INDEX_NAME = 'lenta-news-index'

from flask import Flask
from flask import render_template
from flask import request
from flask import redirect
from flask import url_for
from flask import escape


app = Flask(__name__)


import news_collector
CATEGORIES = news_collector.CATEGORIES
NEWS_CLASSIF = pickle.load(open("news_category_classif.pickle", "rb"))

@app.route('/')
def h_main():
    html = u"""
        <!DOCTYPE HTML>
        <html>
            <head>
            <meta charset="utf-8"> 
            </head>
            <body>
               <p><a href="%(classif)s">Классификатор</a></p>
               <p><a href="%(search)s">Поиск</a></p>
            </body>
        </html> """ % dict( classif=url_for('h_classif')
                          , search=url_for('h_search') )
    return html.encode("utf8")

@app.route('/search/')
def h_search():
    html = u"""
        <!DOCTYPE HTML>
        <html>
            <head>
            <meta charset="utf-8"> 
            </head>
            <body>
               <form action="/search/result" method="get" >
                   <input type="text" name="keywords">
                   <input type="submit" value="Искать в новостях">
               </form>                
               <p><a href="%(frontpage)s">В начало</a></p>
            </body>
        </html>
    """ % dict( frontpage=url_for('h_main'))
    return html.encode("utf8")

@app.route('/search/result')
def h_search_result():
    q = request.args.get('keywords')
    es = Elasticsearch()
    if not es.ping():
        return "Для поиска нужно запустить elasticseach сервер."
    a = es.search(index=INDEX_NAME, doc_type='news', q=q)
    if not a["hits"]["total"]:
        results_html = u"<p>По Вашему запросу ничего не найдено.</p>"
    else:
        results_html = []
        for h in a["hits"]["hits"]:
            results_html.append(u'<p><a href="%(url)s">%(title)s</a></p>' % h["_source"])
        results_html = u"".join(results_html)
    html = u"""
        <!DOCTYPE HTML>
        <html>
            <head>
            <meta charset="utf-8"> 
            </head>
            <body>
               <p>%(q)s</p>
               %(results_html)s
               <p><a href="%(frontpage)s">В начало</a></p>
            </body>
        </html> """ % dict( frontpage=url_for('h_main')
                          , q = escape(request.args.get('keywords'))
                          , results_html = results_html)
    return html.encode("utf8")

    
@app.route('/classif/')
def h_classif():
    html = u"""
        <!DOCTYPE HTML>
        <html>
            <head>
            <meta charset="utf-8"> 
            </head>
            <body>
               <form action="/classif/result" method="get" >
                   <p>Напечатайте текст новости и нажмите отправить.</p>
                   <div><textarea name="input"></textarea></div>
                   <div><input type="submit" value="Отправить"></div>
               </form>                
               <p><a href="%(frontpage)s">В начало</a></p>
            </body>
        </html> """ % dict( frontpage=url_for('h_main'))
    return html.encode("utf8")                      
    
@app.route('/classif/result')
def h_classif_result():
    input = request.args.get('input')
    html = u"""
        <!DOCTYPE HTML>
        <html>
            <head>
            <meta charset="utf-8"> 
            </head>
            <body>
               <form action="/classif/result" method="get" >
                   <p>%(category)s</p>
                   <div><textarea name="input">%(text)s</textarea></div>
                   <div><input type="submit" value="Отправить"></div>
               </form>                
               <p><a href="%(frontpage)s">В начало</a></p>
            </body>
        </html> """ % dict( frontpage=url_for('h_main')
                          , text = escape(input)
                          , category=CATEGORIES[str(NEWS_CLASSIF.predict([input])[0])])
    return html.encode("utf8")
    
if __name__ == "__main__":
    # Проверяем что elasticsearch запущен.
    es = Elasticsearch()
    if not es.ping():
        print "Start elasticsearch server befor."
        raw_input("Press enter ...")
        import sys
        sys.exit()
    else:
        del es
   
    app.run()
     