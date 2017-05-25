#!/usr/bin/python
# -*- coding:utf-8 -*-
import urllib2
import datetime
from bs4 import BeautifulSoup
import MySQLdb
import tweepy
import re
import HTMLParser
from tweepy import OAuthHandler
conn= MySQLdb.connect(
        host='172.18.100.5',
        port = 3306,
        user='jobadmin',
        passwd='jobadmin',
        db ='twitter',
        charset = 'utf8'
        )
proxies = {
    'http': 'http://127.0.0.1:8118',
    'https': 'http://127.0.0.1:8118',
}
proxy_handler=urllib2.ProxyHandler(proxies)
opener=urllib2.build_opener(proxy_handler)
urllib2.install_opener(opener)
html_parser = HTMLParser.HTMLParser()
consumer_key = "N12louSFYDo2dgb7bhHBTge7a"
consumer_secret = "4QcEojhonsSYSekLmd879eYQkaS6ICiT25escEZYCWgi9hc3Uz"
access_key = "839320943520428033-UqbP3kyiOicYiQPqTxo2mNW5e6s6fit"
access_secret = "hBp6FJnP2gjF8YXtLjuzSWehig2W9oXWaLCPAOOEJ7E8t"
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_key,access_secret)
api=tweepy.API(auth)
api.proxy=proxies
def filter_emoji(desstr,restr=''):
    try:  
        co = re.compile(u'[\U00010000-\U0010ffff]')  
    except re.error:  
        co = re.compile(u'[\uD800-\uDBFF][\uDC00-\uDFFF]')  
    return co.sub(restr, desstr)
def getxuanwu_user():
    user_list=[]
    day=datetime.datetime.today()
    url="https://xuanwulab.github.io/cn/secnews/"+day.strftime('%Y/%m/%d')+"/index.html"
    response = urllib2.urlopen(url)
    html = response.read()
    soup = BeautifulSoup(html,"lxml")
    select=soup.select("#singleweiboauthor p")
    for item in select:
        screen_name=item.get_text().split('@')[-1].strip()
        user_list.append(screen_name)
    return user_list

def adduser(user_list):
    for screen_name in user_list:
        user_exist=True
        while True:
            try:
                user=api.get_user(screen_name)
                id=user.id
                nickname=html_parser.unescape(filter_emoji(user.name)).encode("UTF-8", errors='ignore')
                time_zone=user.time_zone
                if time_zone:
                    time_zone=time_zone.encode('utf8',errors='ignore')
                utc_offset=user.utc_offset       
                created_at=user.created_at  
                description=user.description
                if description:
                    description=html_parser.unescape(filter_emoji(description)).encode("UTF-8", errors='ignore')
                location= user.location
                if location:
                    location=html_parser.unescape(filter_emoji(location)).encode("UTF-8", errors='ignore')              
                statuses_count=user.statuses_count
                friends_count=user.friends_count
                followers_count=user.followers_count
                favourites_count=user.favourites_count
                verified=user.verified
                break
            except tweepy.TweepError, e:
                print 'TweepError when get get_user(screen_name='+screen_name+'):\n',e
                if "User not found." in str(e) or "User has been suspended." in str(e):
                    user_exist=False
                    break
                time.sleep(10)
        if not user_exist:
            continue
        params=(screen_name,id,nickname,time_zone,utc_offset,created_at,description,location,statuses_count,friends_count,followers_count,favourites_count,verified,\
                nickname,time_zone,utc_offset,description,location,statuses_count,friends_count,followers_count,favourites_count,verified)
        cur = conn.cursor()
        cur.execute("insert into twitter_users"+\
            "(user,count,user_id,nickname,time_zone,utc_offset,created_at,description,location,statuses_count,friends_count,followers_count,favourites_count,verified)"+\
            " values(%s,1,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "+\
            "on duplicate key update count=count+1,"+\
            "nickname=%s,time_zone=%s,utc_offset=%s,description=%s,location=%s,statuses_count=%s,friends_count=%s,followers_count=%s,favourites_count=%s,verified=%s",params)
        cur.close()
        conn.commit()
        print "add "+screen_name

if __name__ == '__main__':
    i=1
    while True:
        try:
            user_list=getxuanwu_user()
            break
        except Exception,e:
            print e
            i=i+1
            if i>10:
                print "lost "+datetime.datetime.today().strftime('%Y/%m/%d') 
                sys.quit(0)    
    adduser(user_list)
    conn.close()
