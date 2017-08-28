import os
import random
import requests
import json
from bs4 import BeautifulSoup
import time
#import sys
import traceback
import threading
import redis
import re
from pymongo import MongoClient
import socket
import tieba_fetch_bySort
import arrow
import dateutil
import datetime

def parser_time(_time):
    #tz = 'Asia/Hong_Kong'
    tz = 'Asia/BeiJing'
    if _time.find(':') >0:
        last_reply_at =  arrow.get(arrow.now().format('YYYY-MM-DD')+' '+_time,'YYYY-MM-DD HH:mm').replace(tzinfo=dateutil.tz.gettz(tz)).timestamp
    elif _time.find('-') >0:
        m,d = _time.split('-')
        if len(m) == 1:
            m = '0'+m
        if len(d) == 1:
            d = '0'+d
        last_reply_at = arrow.get(arrow.now().format('YYYY-') + '-'.join([m,d]), 'YYYY-MM-DD').replace(tzinfo=dateutil.tz.gettz(tz)).timestamp
    else:
        last_reply_at = parser_time('1970-01-01 00:00')
    if last_reply_at > int(time.time()):
        last_reply_at-=(365*24*3600)
    return last_reply_at*1000


def item_perk(tie_list,pool):
    try:
        rcli=redis.StrictRedis(connection_pool=pool)
        for tie in tie_list:           
            if tie and len(tie.keys()):
                rcli.rpush('tieba_untreated_tie',tie)
        print('Based information fetching post has been completed, waiting for completion!')
    except:
        traceback.print_exc()
                

def parserAndStorage_ties(ties,pool,db):
    try:
        rcli = redis.StrictRedis(connection_pool=pool)
        if ties and len(ties.keys()):
            tie_list=[]
            ba_name=ties['ba_name']
            ties=ties['ties']
            for tie in ties:
                data_field=tie.get('data-field').replace('false', 'False').replace('true', 'True').replace('null', 'None')
                last_reply=tie.select('div.t_con div.j_threadlist_li_right div.threadlist_detail div.threadlist_author span.threadlist_reply_date')
                authpr_info=tie.select('span.tb_icon_author')
                tie_url='http://tieba.baidu.com'+tie.select('div.threadlist_title a.j_th_tit')[0].get('href')
                tiezi={
                    'tieba_id':ba_name,
                    'author_name':eval(data_field)['author_name'],
                    'reply_num':eval(data_field)['reply_num'],
                    'id':str(eval(data_field)['id']),
                    'title':tie.select('div.threadlist_title a.j_th_tit')[0].get('title'),
                    'tie_url':tie_url,
                    'author_id':str(json.loads(tie.select('span.tb_icon_author')[0].get('data-field'))['user_id']) if len(authpr_info) else '',
                    'last_reply_at':parser_time(last_reply[0].text.strip()) if len(last_reply) else parser_time('00:00')
                }
                created_at=rcli.hget('tieba_created_at_hash',ba_name)
                if created_at and tiezi['last_reply_at'] < int(created_at.decode()):
                    item_perk(tie_list,pool)
                    return False
                elif tiezi['last_reply_at'] < int(time.time()*1000)-(7*24*3600*1000):
                    item_perk(tie_list,pool)
                    return False
                tie_list.append(tiezi)
            item_perk(tie_list,pool)
            return True
        else:
            return False
    except:
        traceback.print_exc()
        return True

def tiebaInfo_fetch(bs,db,name):
    conn=db.tieba_info
    today=datetime.date.today()
    version=int(time.mktime(today.timetuple()))*1000
    if not conn.find_one({'name':name,'version':version}):
        spans=bs.select('div.head_main div.card_title div.card_num span')
        try:
            ba_m_num=int(spans[0].select('.card_menNum')[0].text.replace(',','').strip()) if len(spans[0].select('.card_menNum')) else 0
            ba_p_num=int(spans[0].select('.card_infoNum')[0].text.replace(',','').strip()) if len(spans[0].select('.card_infoNum')) else 0
        except IndexError:
            print('{name} this tieba url, abnormal characters, cannot be accessed!'.format(name=name))
            return
        conn.insert({'name':name,'ba_m_num':ba_m_num,'ba_p_num':ba_p_num,'version':version})
        print('{name} {today} tieba_info update is successful'.format(name=name,today=today.strftime("%Y-%m-%d")))
    else:
        print('{name} {today} tieba_info was updated'.format(name=name,today=today.strftime("%Y-%m-%d")))

    

def fetch_tiezi(pool,db1,db2):
    rcli = redis.StrictRedis(connection_pool=pool)
    while True:
        try:
            if db1.client.is_primary :
                db=db1
            elif db2.client.is_primary :
                db = db2
            item = eval(rcli.brpoplpush('tieba_url_list','tieba_url_list',0).decode())
            url=item['url']+'&rn=100'
            ba_name=item['name']
            res=requests.get(url,timeout=15)
            try:
                bs=BeautifulSoup(res.content.decode('utf-8'), 'html.parser')
            except UnicodeDecodeError:
                bs=BeautifulSoup(res.text, 'html.parser')
            tiebaInfo_fetch_thread=threading.Thread(target=tiebaInfo_fetch,args=(bs,db,ba_name))
            tiebaInfo_fetch_thread.start()
            ties=bs.select('li[data-field]')
            ties={'ba_name':ba_name,'ties':ties}
            print('Post information is caught, wait to parse!')
            isContinue=parserAndStorage_ties(ties,pool,db)           
            if isContinue:
                _page=bs.select('div#frs_list_pager a.last')
                if not len(_page):
                    continue
                max_page=int(re.findall(r'\d+',_page[0]['href'])[-1])
                pnum=100
                while pnum<=max_page and isContinue :
                    url_p=url+'&pn={pnum}'.format(pnum=pnum)
                    res=requests.get(url_p,timeout=15)
                    bs=BeautifulSoup(res.content.decode('utf-8'), 'html.parser')
                    ties=bs.select('li[data-field]')
                    ties={'ba_name':ba_name,'ties':ties}
                    print('Post information is caught, wait to parse!')
                    isContinue=parserAndStorage_ties(ties,pool,db)
                    pnum+=100
            rcli.hset('tieba_created_at_hash',ba_name,int(time.time()*1000))
            tiebaInfo_fetch_thread.join()
            time.sleep(4)
        except:
            traceback.print_exc()
