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


def fetch_hot_topic():
    url='http://tieba.baidu.com/hottopic/browse/topicList'
    res=requests.get(url,timeout=15)
    text=res.content.decode('utf-8')
    _data=json.loads(text)
    if ('data' in _data.keys()) and ('bang_topic' in _data['data'].keys()):
        topic_list=_data['data']['bang_topic']['topic_list']
        topics=[]
        for topic in topic_list:
            topic={
                'topic_id':topic['topic_id'],
                'topic_name':topic['topic_name'],
                'abstract':topic['abstract'],
                'topic_avatar':topic['topic_avatar'],
                'discuss_num':topic['discuss_num'],
                'idx_num':topic['idx_num'],
                'topic_url':topic['topic_url'].strip().replace('\/','/')
                }
            topics.append(topic)
    print(topics[0])