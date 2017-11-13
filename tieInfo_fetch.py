import os
import random
import requests
#import json
from bs4 import BeautifulSoup
import time
#import sys
import json
import traceback
import threading
import redis
#import re
from pymongo import MongoClient
from elasticsearch import helpers
from elasticsearch import Elasticsearch
import socket
import tiezi_fetch
import arrow
import dateutil

def tie_into_es(pool,es):
    print('Write Elasticsearch thread start to work!')
    rcli=redis.StrictRedis(connection_pool=pool)
    into_es=[]
    headers={'Content-Type':'application/json'}
    while True:
        try:
            if rcli.llen('tie2es_list')>=500:
                while rcli.llen('tie2es_list')>0:
                    _item=rcli.rpop('tie2es_list')
                    if _item!=None and _item!={}:
                        item = eval(_item.decode())
                        item['index_name']='tieba_posts'
                        item['type_name']='tieba_posts'
                        into_es.append(item)
                    if len(into_es)>=20:
                        #print(into_es)
                        #helpers.bulk(es, into_es, index='ties_es',doc_type='ties_docType_es', raise_on_error=True)
                        requests.post('http://59.110.52.213/stq/api/v1/pa/baidutieba/add',headers=headers,data=json.dumps(into_es))
                        print(str(len(into_es))+' into Elasticsearch')
                        del into_es[0:len(into_es)]
                if len(into_es) >0:
                    #print(into_es)
                    #helpers.bulk(es, into_es, index='ties_es',doc_type="ties_docType_es", raise_on_error=True)
                    requests.post('http://59.110.52.213/stq/api/v1/pa/baidutieba/add',headers=headers,data=json.dumps(into_es))
                    print(str(len(into_es))+' into Elasticsearch')
                    del into_es[0:len(into_es)]
        except:
            if len(into_es) > 0:
                #helpers.bulk(es, into_es, index='ties_es',doc_type="ties_docType_es", raise_on_error=True)
                requests.post('http://59.110.52.213/stq/api/v1/pa/baidutieba/add',headers=headers,data=json.dumps(into_es))
                print(str(len(into_es))+' into Elasticsearch')
                del into_es[0:len(into_es)]
            traceback.print_exc()
        time.sleep(3)
        

def fetch_tieInfo(pool,db1,db2,es):
    print('fetch_tieInfo started!')
    rcli=redis.StrictRedis(connection_pool=pool)  
    while True:
        try:
            if db1.client.is_primary :
                db=db1
            elif db2.client.is_primary :
                db = db2
            conn=db.ties
            tie = eval(rcli.brpop('tieba_untreated_tie',0)[1].decode())
            print(tie)
            if tie!=None and len(tie.keys())!=0:
                url=tie['tie_url']
                res=requests.get(url,timeout=15)
                bs=BeautifulSoup(res.text, 'html.parser')
                boundaries=bs.select('div[data-field]')
                if len(boundaries):
                    boundaries=boundaries[0]
                    json_data=json.loads(boundaries.get('data-field'))
                    author_id=tie['author_id']
                    if author_id=='':
                        json_author=json_data['author']
                        author_id=str(json_author['user_id']) if 'user_id' in json_author.keys() else ''
                    json_content=json_data['content']
                    create_time=json_content['date'] if 'date' in json_content.keys() else boundaries.select('div[class="post-tail-wrap"] span[class="tail-info"]')[-1].text
                    post_id=json_data['content']['post_id']
                    post_content=boundaries.select('#post_content_{post_id}'.format(post_id=post_id))[0]
                    _content=post_content.text.strip()
                    tie['date']=tiezi_fetch.parser_time(create_time)
                    tie['content']=_content
                    tie['author_id']=author_id
                    tie['created_at']=int(time.time()*1000)
                    del tie['tie_url']
                    rcli.lpush('tie2es_list',tie)
                    print(tie['id'],'_This post has been completion information and deposited in the redis, ready to push the Elasticsearch!')
                #time.sleep(4)
        except:
            traceback.print_exc()
