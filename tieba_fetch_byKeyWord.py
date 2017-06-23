import os
#import random
import requests
#import json
from bs4 import BeautifulSoup
import time
#import sys
import traceback
import threading
import redis
import re
from pymongo import MongoClient
import tieba_fetch_bySort

def readWords(rcli):
    try:
        with open(os.path.abspath('.')+'/keyWords/words.txt','r',-1,encoding='utf-8',errors='ignore') as f:
            words=f.read().split(' ')	
            for word in words:
                rcli.lpush('tieba_keyWords',word)
    except:
        traceback.print_exc()

def getKeyWord(rcli):
	try:
		if rcli.llen('tieba_keyWords')==0:
			readWords(rcli)
		word=rcli.brpoplpush('tieba_keyWords','tieba_keyWords',0).decode('utf-8')
		return word
	except:
		traceback.print_exc()

def tags_parser(tags,pool,db):
    try:
        tiebas=[]
        for tag in tags:
            url='http://tieba.baidu.com'+tag.select('div[class=forum-name-wraper] a[target=_blank]')[0].get('href')
            ba_n=tag.select('div[class=forum-name-wraper] a[forum-name]')
            ba_p_m=tag.select('div[class=forum-post-num-wraper] span')
            ba_name=ba_n[0].get('forum-name').strip() if len(ba_n) else ''
            ba_m_num=ba_p_m[1].text if len(ba_p_m)>=4 else ''
            ba_p_num=ba_p_m[3].text if len(ba_p_m)>=4 else ''
            if ba_name=='' or ba_m_num=='' or ba_m_num is None or ba_p_num=='' or ba_p_num is None:
                _num=tieba_fetch_bySort.supplement(url)
                if _num:
                    ba_m_num=_num['ba_m_num']
                    ba_p_num=_num['ba_p_num']
                    ba_name=_num['ba_name']
            tieba={
                '_id':ba_name,
                'ba_url':url,
                'ba_m_num':int(ba_m_num),
                'ba_p_num':int(ba_p_num)
                }
            if ba_name!='':
                tiebas.append(tieba)
        tieba_fetch_bySort.item_into_mongo(tiebas,db,pool)
    except:
        traceback.print_exc()

def fetch_byKeyWord(pool,db1,db2):
    rcli = redis.StrictRedis(connection_pool=pool)
    while True:
        try:       
            if db1.client.is_primary :
                db=db1
            elif db2.client.is_primary :
                db = db2
            word=getKeyWord(rcli)
            #print('Have to get the keywords:',word,'Start fetching posted links!')
            url='http://tieba.baidu.com/f/search/fm?ie=UTF-8&qw='+word+'&rn=1'
            res=requests.get(url,timeout=15)
            bs=BeautifulSoup(res.text, 'html.parser')
            max_info=bs.select('div[class=wrap2] div.pager-search span.s_nav_right')
            if len(max_info):
                max_count=re.findall(r'\d+',max_info[0].text)[0]
                url='http://tieba.baidu.com/f/search/fm?ie=UTF-8&qw='+word+'&rn='+max_count
                res=requests.get(url,timeout=15)
                bs=BeautifulSoup(res.text, 'html.parser')
                tags=bs.select('div[class=wrap2] div[class=search-forum-list] .forum-item div[class=right]')
                tags_parser_thread=threading.Thread(target=tags_parser, args=(tags,pool,db))
                tags_parser_thread.start()
                time.sleep(3)
                tags_parser_thread.join(3)
                print('Post bar link has caught!')
        except:
            traceback.print_exc()
