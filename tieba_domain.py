from pymongo import MongoClient
import threading
import redis
import os
import time
import traceback
from elasticsearch import Elasticsearch
import socket
import random
import check_repetition
import tieba_fetch_byKeyWord
import tieba_fetch_bySort
import tiezi_fetch
import tieInfo_fetch


def all_fetcher_thread(rpool, db1,db2,es):
    for i in range(2):
        t1=threading.Thread(target=tieba_fetch_bySort.fetch_bySort,args=(rpool, db1,db2))
        t2=threading.Thread(target=tieba_fetch_byKeyWord.fetch_byKeyWord,args=(rpool, db1,db2))
        t3=threading.Thread(target=check_repetition.check_cache,args=(rpool,))
        t1.start()
        t2.start()
        t3.start()

    for i in range(2):
        print('Began to grab post information!')
        t1=threading.Thread(target=tiezi_fetch.fetch_tiezi,args=(rpool, db1,db2))
        t2=threading.Thread(target=tieInfo_fetch.fetch_tieInfo,args=(rpool, db1,db2,es))
        t3=threading.Thread(target=tieInfo_fetch.tie_into_es,args=(rpool,es))
        t1.start()
        t2.start()
        t3.start()



def do_main():
    with open(os.path.abspath('.') + '/Redis_Mongo_Es' + '/redis_mongo_es.txt', 'r', encoding='utf-8') as f:
        line=f.read()
    _dict = eval(line)
    red_dict = _dict['red']
    mon_dict = _dict['mon1']
    mon_dict2 = _dict['mon2']
    es_dict = _dict['es']
    red_host = red_dict['host']
    red_port = int(red_dict['port'])
    red_pwd = red_dict['password']
    mon_host = mon_dict['host']
    mon_port = str(mon_dict['port'])
    mon_user = mon_dict['user']
    mon_pwd = mon_dict['password']
    mon_dn = mon_dict['db_name']
    mon2_host = mon_dict2['host']
    mon2_port = str(mon_dict2['port'])
    mon2_user = mon_dict2['user']
    mon2_pwd = mon_dict2['password']
    mon2_dn = mon_dict2['db_name']
    es_url=es_dict['url']
    es_port=es_dict['port']
    es_name=es_dict['name']
    es_pwd=es_dict['password']
    mon_url='mongodb://' + mon_user + ':' + mon_pwd + '@' + mon_host + ':' + mon_port +'/'+ mon_dn
    mon_url2 = 'mongodb://' + mon2_user + ':' + mon2_pwd + '@' + mon2_host + ':' + mon2_port + '/' + mon2_dn
    rpool = redis.ConnectionPool(host=red_host, port=red_port,password=red_pwd)
    #rpool = redis.ConnectionPool(host='127.0.0.1', port=6379)
    es = Elasticsearch([es_url], http_auth=(es_name, es_pwd), port=es_port)
    #es = Elasticsearch([{'host': '127.0.0.1', 'port': 9200}])
    #mcli = MongoClient('127.0.0.1', 27017)
    #mcli2 = MongoClient('127.0.0.1', 27017)
    mcli = MongoClient(mon_url)
    mcli2 = MongoClient(mon_url2)
    db1 = mcli.get_database('baidutieba')
    db2 = mcli2.get_database('baidutieba')
    working_thread = threading.Thread(target=all_fetcher_thread, args=(rpool, db1,db2,es))
    working_thread.start()
    print('Tieba crawlers start to work!')
    working_thread.join()
if __name__=='__main__':
    try:
        do_main()
    except:
        traceback.print_exc()
