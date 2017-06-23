#import os
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

def Url_into_redis(pool,item_list,name):
    try:
        rcli = redis.StrictRedis(connection_pool=pool)      
        for item in item_list:
            rcli.rpush(name,item)
        print('Post links have been deposited in the redis')
    except:
        traceback.print_exc()

def item_into_mongo(items,db,pool):
    try:
        conn1=db.tiebas
        conn2=db.tieba_info
        tieba_info=[]
        for item in items:
            name=item['_id']
            url=item['ba_url']
            if name!=None and name!='' and url!=None and url!='':
                check_be=conn1.find_one({'_id': name})
                if check_be:
                    tieba_info.append({'name': name,'version':int(time.time()*1000),'ba_m_num':item['ba_m_num'],'ba_p_num':item['ba_p_num']})
                    conn1.update({'_id': name}, item, True)
                else:
                    tieba_info.append({'name': name,'version':int(time.time()*1000),'ba_m_num':item['ba_m_num'],'ba_p_num':item['ba_p_num']})
                    conn1.update({'_id': name}, item, True)
                    Url_into_redis(pool,[{'url':url,'name':name}],'tieba_url_cache')
        if len(tieba_info):
            conn2.insert(tieba_info)
        print('Post bar information is deposited in the mongo!')
    except:
        traceback.print_exc()

def tiebaSort_fetch(pool):
    try:
        #rcli = redis.StrictRedis(connection_pool=pool)
        url='http://tieba.baidu.com/f/index/forumclass'
        res=requests.get(url,timeout=30)
        bs=BeautifulSoup(res.content.decode('utf-8'), 'html.parser')
        items=bs.select('#right-sec .clearfix .class-item')[0:-1]
        item_list=list('http://tieba.baidu.com'+x.select('a[class=class-item-title]')[0].get('href') for x in items)
        item_lis=bs.select('#right-sec .clearfix .class-item .item-list-ul li')[0:-3]
        item_list+=list('http://tieba.baidu.com'+x.find_all('a')[0].get('href')+'&rn=300' for x in item_lis)
        Url_into_redis(pool,item_list,'tiebaSortUrl_list')
    except:
        traceback.print_exc()

def supplement(url):
    try:
        res=requests.get(url,timeout=15)
        try:
            bs=BeautifulSoup(res.content.decode('utf-8'), 'html.parser')
        except UnicodeDecodeError:
            bs=BeautifulSoup(res.text, 'html.parser')
        head=bs.select('div[class=header] div[class=card_num] span[class=""]')
        f_name=bs.select('div[class=header] div[class=head_content] div[class=card_title] a[class=" card_title_fname"]')
        f_name=f_name[0].text.strip() if len(f_name) else ''
        if len(head) and f_name:
            men_num=head[0].select('span[class=card_menNum]')
            info_num=head[0].select('span[class=card_infoNum]')
            m_num=men_num[0].text.replace(',','').strip() if len(men_num) else '0'
            p_num=info_num[0].text.replace(',','').strip() if len(info_num) else '0'
            return {'ba_name':f_name,'ba_m_num':m_num,'ba_p_num':p_num}
    except:
        traceback.print_exc()
        return False

def parserAndStorage_items(items,pool,db):
    try:
        tiebas=[]
        for item in items:
            ba_m=item.select('div[class=ba_content] p.ba_num span[class=ba_m_num]')
            ba_p=item.select('div[class=ba_content] p.ba_num span[class=ba_p_num]')
            ba_n=item.select('div[class=ba_content] p[class=ba_name]')
            ba_name=ba_n[0].text.strip() if len(ba_n) else ''
            ba_m_num=ba_m[0].text.strip() if len(ba_m) else ''
            ba_p_num=ba_p[0].text.strip() if len(ba_p) else ''
            if ba_name=='' or ba_m_num=='' or ba_m_num is None or ba_p_num=='' or ba_p_num is None:
                _num=supplement('http://tieba.baidu.com'+item.get('href'))
                if _num:
                    ba_m_num=_num['ba_m_num']
                    ba_p_num=_num['ba_p_num']
                    ba_name=_num['ba_name']
            tieba={
                '_id':ba_name,
                'ba_url':'http://tieba.baidu.com'+item.get('href'),
                'ba_m_num':int(ba_m_num) if ba_m_num.isdecimal() else 0,
                'ba_p_num':int(ba_p_num) if ba_p_num.isdecimal() else 0
            }
            if ba_name!='':
                tiebas.append(tieba)
            #time.sleep(30)
        print('Tieba information parsed, waiting for storage!')
        item_into_mongo(tiebas,db,pool)
    except:
        traceback.print_exc()
    
    

def fetch_bySort(pool,db1,db2):
    rcli = redis.StrictRedis(connection_pool=pool)
    if not rcli.llen('tiebaSortUrl_list'):
        tiebaSort_fetch(pool)
        print('Classification link has caught!')
    while True:
        try:
            if db1.client.is_primary :
                db=db1
            elif db2.client.is_primary :
                db = db2           
            url = rcli.brpoplpush('tiebaSortUrl_list','tiebaSortUrl_list',0).decode()
            res=requests.get(url,timeout=15)
            bs=BeautifulSoup(res.content.decode('utf-8'), 'html.parser')
            items=bs.select('#ba_list .ba_info a[target=_blank]')
            parserAndStorage_thread=threading.Thread(target=parserAndStorage_items, args=(items,pool,db))
            parserAndStorage_thread.start()
            pnums=bs.select('.container .content .right-sec .square_pager .pagination a')
            if len(pnums):
                max_pnum=int(re.findall(r'\d+',pnums[-1]['href'])[-1])
            else:
                max_pnum=0
            pnum=2
            _threads=[]
            while pnum<=max_pnum:
                url_p=url+'&pn='+str(pnum)
                res=requests.get(url_p,timeout=15)
                bs=BeautifulSoup(res.content.decode('utf-8'), 'html.parser')
                items=bs.select('#ba_list .ba_info a[target=_blank]')
                pnum+=1
                _thread=threading.Thread(target=parserAndStorage_items, args=(items,pool,db))
                _threads.append(_thread)
                _thread.start()
                time.sleep(3)
            if len(_threads):
                for i in _threads:
                    i.join(5)
            print('Post bar link has caught!')
            parserAndStorage_thread.join(5)
            
        except:
            traceback.print_exc()




