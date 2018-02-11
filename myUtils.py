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
# from elasticsearch import helpers
# from elasticsearch import Elasticsearch
import socket
import tiezi_fetch
import arrow
import dateutil

tieba_logs={'index_name':'tieba_logs','type_name':'tieba_logs'}

def vital_tieba(db1,db2):
    while 1:
        try:
            if db1.client.is_primary :
                db=db1
            else :
                db = db2
            bas=list(db.tiebas.find({'ba_m_num':{'$gte':500000}}))
            start=int(time.time())
            for i in bas:
                ba_url=i['ba_url']
                ba_name=i['_id']
                res=requests.get(ba_url,timeout=30)
                try:
                    bs=BeautifulSoup(res.content.decode('utf-8'), 'html.parser')
                except UnicodeDecodeError:
                    bs=BeautifulSoup(res.text, 'html.parser')
                baInfo=tiezi_fetch.tiebaInfo_fetch(bs,ba_name)
                baInfo=[dict(baInfo,**tieba_logs)]
                requests.post('http://59.110.52.213/stq/api/v1/pa/shareWrite/add',headers={'Content-Type':'application/json'},data=json.dumps(baInfo))
            end=int(time.time())
            time.sleep(12*3600-(end-start))
        except:
            time.sleep(60)
            continue
    


def parse_lreply(bs):
    boundaries=bs.select('div[data-field] div.post-tail-wrap')
    boundarie=boundaries[-1].select('span.tail-info')[-1].text.strip() if len(boundaries) else None
    lreply=tiezi_fetch.parser_time(boundarie) if boundarie else None
    return lreply


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


def tieInfo_fetch(tie,db):
    while 1:
        try:
            url=tie['tie_url']
            res=requests.get(url,timeout=15)
            try:
                bs=BeautifulSoup(res.content.decode('utf-8'), 'html.parser')
            except UnicodeDecodeError:
                bs=BeautifulSoup(res.text, 'html.parser')
            doodle=bs.select_one('.page404')
            if doodle:
                db.tieba_err_ties.update({'_id':tie['id']},tie,True)
                print(tie['id'],'into mongo the tieba_err_ties')
                del tie['tie_url']
                return
            lreply=get_last_reply(url,bs)
            data_field=bs.select_one('div[data-field]')
            if data_field:
                #boundaries=boundaries[0]
                boundarie=data_field.get('data-field')
                json_data=json.loads(boundarie)
                author_id=tie['author_id']
                if author_id=='':
                    json_author=json_data['author']
                    author_id=str(json_author['user_id']) if 'user_id' in json_author.keys() else ''
                json_content=json_data['content']
                create_time=json_content['date'] if 'date' in json_content.keys() else data_field.select('div.post-tail-wrap span.tail-info')[-1].text if len(data_field.select('div.post-tail-wrap span.tail-info')) else bs.select_one('.post-tail-wrap').select('span')[-1].text
                post_id=json_data['content']['post_id']
                post_content=data_field.select_one('#post_content_{post_id}'.format(post_id=post_id))
                _content=post_content.text.strip()
                tie['date']=tiezi_fetch.parser_time(create_time)
                tie['content']=tiezi_fetch.remove_emoji(_content)
                tie['author_id']=author_id
                tie['created_at']=int(time.time()*1000)
                tie['last_reply_at']=lreply if lreply else tie['last_reply_at']
                del tie['tie_url']
                return tie
                #rcli.lpush('tie2es_list',tie)
                #db.tie2es.update({'_id':tie['id']},tie,True)
                #db.tieba_undeal_ties.remove({'_id':tie['id']})
                #print(tie['id'],'_This post has been completion information and deposited in the redis, ready to push the Elasticsearch!')
            else:
                db.tieba_err_ties.update({'_id':tie['id']},tie,True)
                print(tie['id'],'into mongo the tieba_err_ties')
                del tie['tie_url']
                return
        except:
            traceback.print_exc()
            continue


