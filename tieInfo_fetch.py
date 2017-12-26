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


def parse_lreply(bs):
    boundaries=bs.select('div[data-field] div.post-tail-wrap')
    boundarie=boundaries[-1].select('span.tail-info')[-1].text.strip() if len(boundaries) else None
    lreply=tiezi_fetch.parser_time(boundarie) if boundarie else None
    return lreply

# def load_bound(bs,url,rcli):
#     boundaries=bs.select('div[data-field]')
#     if len(boundaries):
#         boundarie=boundaries[0].get('data-field')
#         rcli.hset('boundaries',url,boundarie)  

def get_last_reply(url,bs):
    while 1:
        try:
            #bs=BeautifulSoup(res.text, 'html.parser')
            #load_bound(bs,url,rcli)
            max_place=bs.select_one('#thread_theme_5 li.l_reply_num > input#jumpPage4')
            if max_place:
                res=requests.get('{}?pn={}'.format(url,max_place.get('max-page')))
                try:
                    bs=BeautifulSoup(res.content.decode('utf-8'), 'html.parser')
                except UnicodeDecodeError:
                    bs=BeautifulSoup(res.text, 'html.parser')
                return parse_lreply(bs)
            else:
                return parse_lreply(bs)
        except:
            traceback.print_exc()
            continue


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
            #print(tie)
            flag=tie['flag']
            if len(tie.keys())!=0:
                try:
                    url=tie['tie_url']
                    res=requests.get(url,timeout=15)
                    try:
                        bs=BeautifulSoup(res.content.decode('utf-8'), 'html.parser')
                    except UnicodeDecodeError:
                        bs=BeautifulSoup(res.text, 'html.parser')
                    lreply=get_last_reply(url,bs)
                    boundaries=bs.select_one('div[data-field]')
                    if boundaries:
                        #boundaries=boundaries[0]
                        boundarie=boundaries.get('data-field')
                        json_data=json.loads(boundarie)
                        author_id=tie['author_id']
                        if author_id=='':
                            json_author=json_data['author']
                            author_id=str(json_author['user_id']) if 'user_id' in json_author.keys() else ''
                        json_content=json_data['content']
                        create_time=json_content['date'] if 'date' in json_content.keys() else boundaries.select('div.post-tail-wrap span.tail-info')[-1].text if len(boundaries.select('div.post-tail-wrap span.tail-info')) else bs.select('.post-tail-wrap')[0].select('span')[-1].text
                        post_id=json_data['content']['post_id']
                        post_content=boundaries.select('#post_content_{post_id}'.format(post_id=post_id))[0]
                        _content=post_content.text.strip()
                        tie['date']=tiezi_fetch.parser_time(create_time)
                        tie['content']=_content
                        tie['author_id']=author_id
                        tie['created_at']=int(time.time()*1000)
                        tie['last_reply_at']=lreply if lreply else tie['last_reply_at']
                        del tie['tie_url']
                        del tie['flag']
                        rcli.lpush('tie2es_list',tie)
                        print(tie['id'],'_This post has been completion information and deposited in the redis, ready to push the Elasticsearch!')
                    else:
                        tie['flag']=flag+1
                        rcli.lpush('tieba_untreated_tie',tie)
                except:
                    traceback.print_exc()
                    print(tie['title'],tie['tie_url'])
                    rcli.lpush('tieba_untreated_tie',tie)
                #time.sleep(4)
        except:
            traceback.print_exc()
