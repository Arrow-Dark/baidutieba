import os
import random
import requests
#import json
#from bs4 import BeautifulSoup
import time
#import sys
import traceback
import threading
import redis
import re
from pymongo import MongoClient
import socket


def eliminate_repetition_intoRedis(rcli):
    hostName = socket.gethostname()
    count=0
    while count<=1000:
        try:
            if not rcli.llen('tieba_url_cache'):
                break
            item=eval(rcli.rpop('tieba_url_cache').decode())
            count+=1
            if item and len(item.keys()):
                if not rcli.sismember('tieba_url_set', item):
                        with rcli.pipeline() as pipe:
                            pipe.multi()
                            pipe.sadd('tieba_url_set', item)
                            pipe.rpush('tieba_url_list', item)
                            pipe.execute()
                        print('Get a tieba, and deposited in the queue!')
        except redis.exceptions.ConnectionError:
            print(hostName + ': With redis connection is broken!')
            traceback.print_exc()
        except:
            traceback.print_exc()
            break

def check_start(pool):
    try:
        rcli = redis.StrictRedis(connection_pool=pool)
        ks=rcli.keys()
        if b'_bump' not in ks and b'_oh' not in rcli.keys():
            rcli.lpush('_bump',1)
        while True:
            try:          
                rcli.brpoplpush('_bump','_oh',0)
                hostName = socket.gethostname()
                eliminate_repetition_intoRedis(rcli)
                rcli.rpoplpush('_oh', '_bump')
                time.sleep(1)
            except redis.exceptions.ConnectionError:
                print(hostName + ': With redis connection is broken!')
                traceback.print_exc()
            except:
                rcli.rpoplpush('_oh', '_bump')
                traceback.print_exc()
    except redis.exceptions.ConnectionError:
            print(hostName + ': With redis connection is broken!')
            traceback.print_exc()
            


def check_ball(pool):
    rcli = redis.StrictRedis(connection_pool=pool)
    count=0
    while True:
        if rcli.llen('_oh'):
            count+=1
            if count>=100:
                rcli.rpoplpush('_oh', '_bump')
        else:
            count=0
        guess=random.randint(1,20)
        time.sleep(guess)

def check_cache(pool):
    t1=threading.Thread(target=check_start,args=(pool,))
    t2 = threading.Thread(target=check_ball, args=(pool,))
    t1.start()
    t2.start()
    #t1.join()
    #t2.join()
