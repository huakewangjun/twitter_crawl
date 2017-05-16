# -*- coding:utf-8 -*-
import gc
import json
import time
import tweepy
import HTMLParser
from tweepy import OAuthHandler
import MySQLdb
import re
import urllib2
from bs4 import BeautifulSoup
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
#目前使用的代理端口为8888，可以根据需要进行修改
proxies = {
    'http': 'http://127.0.0.1:8118',
    'https': 'http://127.0.0.1:8118',
}
proxy_handler=urllib2.ProxyHandler(proxies)
opener=urllib2.build_opener(proxy_handler)
urllib2.install_opener(opener)
conn= MySQLdb.connect(
    host='172.18.100.5',
    port = 3306,
    user='jobadmin',
    passwd='jobadmin',
    db ='twitter',
    charset = 'utf8'
    )
html_parser = HTMLParser.HTMLParser()
total=[0]
def get_users():
    conn= MySQLdb.connect(
        host='172.18.100.5',
        port = 3306,
        user='jobadmin',
        passwd='jobadmin',
        db ='twitter',
        charset = 'utf8'
        )
    cur = conn.cursor()
    cur.execute("select user from twitter_users order by count desc")
    results = cur.fetchall()
    user_list=[]
    for row in results:
        user_name=row[0].encode('utf8')
        user_list.append(user_name)
    cur.close()
    return user_list
def filter_emoji(desstr,restr=''):
    try:  
        co = re.compile(u'[\U00010000-\U0010ffff]')  
    except re.error:  
        co = re.compile(u'[\uD800-\uDBFF][\uDC00-\uDFFF]')  
    return co.sub(restr, desstr)
def get_content_from_html(href):
    req=urllib2.Request(href)
    conn=urllib2.urlopen(req, timeout=120)
    html=conn.read()
    soup = BeautifulSoup(html,"lxml")
    select_content=soup.select(".permalink-tweet .js-tweet-text-container p")[0]
    tags_b=select_content.select(".twitter-hashtag b")
    tags=None
    if tags_b:
        tags_list=[]
        for b in tags_b:
            tags_list.append(b.get_text().encode("UTF-8", errors='ignore'))
            tags=str(tags_list)
    replace_a=select_content.find_all("a",href=re.compile("https?://t.co/\w+"))
    for a in replace_a:
        if 'data-expanded-url' in a.attrs.keys():
            if not a['data-expanded-url'].startswith('https://twitter.com'):
                a.replaceWith(' '+a['href'])
            else:
                a.replaceWith(' ')
        else:
            a.replaceWith(' ')
    return select_content.get_text(),tags
def save_tweet(new_tweet):
    user_name=new_tweet.user.screen_name
    tweet_id=new_tweet.id
    href='https://twitter.com/'+user_name+'/status/'+str(tweet_id)
    created_at=new_tweet.created_at
    favorite_count=new_tweet.favorite_count
    retweet_count=new_tweet.retweet_count
    in_reply_to_status_id=new_tweet.in_reply_to_status_id
    if hasattr(new_tweet,"retweeted_status"):
        retweeted_status_id=new_tweet.retweeted_status.id
        retweeted_status_user=new_tweet.retweeted_status.user.screen_name.encode("UTF-8", errors='ignore')
        retweeted_status_created_at=new_tweet.retweeted_status.created_at
        retweeted_reply_to_status_id=new_tweet.retweeted_status.in_reply_to_status_id

        favorite_count=new_tweet.retweeted_status.favorite_count
        retweet_count=new_tweet.retweeted_status.retweet_count

        retweeted_status_user_id=new_tweet.retweeted_status.user.id
        retweeted_status_user_nickname=html_parser.unescape(filter_emoji(new_tweet.retweeted_status.user.name)).encode("UTF-8", errors='ignore')
        retweeted_status_user_time_zone=new_tweet.retweeted_status.user.time_zone
        if retweeted_status_user_time_zone:
            retweeted_status_user_time_zone=retweeted_status_user_time_zone.encode('utf8',errors='ignore')
        retweeted_status_user_utc_offset=new_tweet.retweeted_status.user.utc_offset       
        retweeted_status_user_created_at=new_tweet.retweeted_status.user.created_at  
        retweeted_status_user_description=new_tweet.retweeted_status.user.description
        if retweeted_status_user_description:
            retweeted_status_user_description=html_parser.unescape(filter_emoji(retweeted_status_user_description)).encode("UTF-8", errors='ignore')
        retweeted_status_user_location= new_tweet.retweeted_status.user.location
        if retweeted_status_user_location:
            retweeted_status_user_location=html_parser.unescape(filter_emoji(retweeted_status_user_location)).encode("UTF-8", errors='ignore')              
        retweeted_status_user_statuses_count=new_tweet.retweeted_status.user.statuses_count
        retweeted_status_user_friends_count=new_tweet.retweeted_status.user.friends_count
        retweeted_status_user_followers_count=new_tweet.retweeted_status.user.followers_count
        retweeted_status_user_favourites_count=new_tweet.retweeted_status.user.favourites_count
        retweeted_status_user_verified=new_tweet.retweeted_status.user.verified
    else:
        retweeted_status_id=None
        retweeted_status_user=None
        retweeted_reply_to_status_id=None
    if hasattr(new_tweet,"quoted_status_id"):
        quoted_status_id=new_tweet.quoted_status_id
    else:
        quoted_status_id=None
    if True:
        while True:
            try:
                content,tags=get_content_from_html(href)
                content=html_parser.unescape(filter_emoji(content)).encode("UTF-8", errors='ignore')
                break
            except Exception as e:
                if "list index out of range":
                    return
                print "error in downloading twitter content from "+href+'\n'+str(e)
                time.sleep(10)
    if re.findall('https?://\S+\.\S+\w',content):
        link=str(re.findall('https?://\S+\.\S+\w',content))
    else:
        link=None
    param=(tweet_id,user_name,content,link,tags,created_at,in_reply_to_status_id,quoted_status_id,retweeted_status_id,retweeted_status_user,retweet_count,favorite_count)
    #将转发的twitter的用户加入用户数据库表
    if retweeted_status_user:
        try:
            param2=(retweeted_status_user,retweeted_status_user_id,retweeted_status_user_nickname,retweeted_status_user_time_zone,retweeted_status_user_utc_offset,\
                    retweeted_status_user_created_at,retweeted_status_user_description,retweeted_status_user_location,retweeted_status_user_statuses_count,\
                    retweeted_status_user_friends_count,retweeted_status_user_followers_count,retweeted_status_user_favourites_count,retweeted_status_user_verified,\
                    retweeted_status_user_nickname,retweeted_status_user_time_zone,retweeted_status_user_utc_offset,\
                    retweeted_status_user_description,retweeted_status_user_location,retweeted_status_user_statuses_count,\
                    retweeted_status_user_friends_count,retweeted_status_user_followers_count,retweeted_status_user_favourites_count,retweeted_status_user_verified)
            conn= MySQLdb.connect(
                host='172.18.100.5',
                port = 3306,
                user='jobadmin',
                passwd='jobadmin',
                db ='twitter',
                charset = 'utf8'
                )
            cur = conn.cursor()
            cur.execute("insert into twitter_users"+\
                        "(user,count,user_id,nickname,time_zone,utc_offset,created_at,description,location,statuses_count,friends_count,followers_count,favourites_count,verified)"+\
                        " values(%s,1,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "+\
                        "on duplicate key update "+\
                        "nickname=%s,time_zone=%s,utc_offset=%s,description=%s,location=%s,statuses_count=%s,friends_count=%s,followers_count=%s,favourites_count=%s,verified=%s",param2)
            conn.commit()
            cur.close()
            print "insert retweet user "+retweeted_status_user
        except Exception as e:
            print "insert retweet user error when user_name="+retweeted_status_user+',params='+str(param2)+":\n"+str(e)
            #sys.exit(1)
        try:
            param1=(retweeted_status_id,retweeted_status_user,content,link,tags,retweeted_status_created_at,retweeted_reply_to_status_id,quoted_status_id,None,None,retweet_count,favorite_count)
            conn= MySQLdb.connect(
                host='172.18.100.5',
                port = 3306,
                user='jobadmin',
                passwd='jobadmin',
                db ='twitter',
                charset = 'utf8'
                )
            cur = conn.cursor()
            cur.execute("insert ignore into twitter_info(id,screen_name,content,link,tags,time,in_reply_to_status_id,quoted_status_id,retweeted_status_id,retweeted_status_user,retweet_count,favorite_count) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",param1) 
            conn.commit()
            cur.close()
            #total[0]=total[0]+1
            print str(total[0])+' '+str(retweeted_status_id)+' '+retweeted_status_user+" insert retweet"
        except Exception as e:
            print "insert retweet into twitter_info error when user_name="+user_name+',params='+str(param1)+":\n"+str(e)
    try:
        conn= MySQLdb.connect(
            host='172.18.100.5',
            port = 3306,
            user='jobadmin',
            passwd='jobadmin',
            db ='twitter',
            charset = 'utf8'
            )
        cur = conn.cursor()
        cur.execute("insert ignore into twitter_info(id,screen_name,content,link,tags,time,in_reply_to_status_id,quoted_status_id,retweeted_status_id,retweeted_status_user,retweet_count,favorite_count) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",param)
        conn.commit()
        cur.close()
    except Exception as e:
        print "insert twitter_info error when user_name="+user_name+',params='+str(param)+":\n"+str(e)
        #sys.exit(1)
    total[0]=total[0]+1
    print str(total[0])+' '+str(tweet_id)+' '+user_name
def creat_threads(new_tweets):
    for i in range(0,len(new_tweets)):
        if not (new_tweets[i].lang is None or new_tweets[i].lang == "en" or new_tweets[i].lang == "zh"):
            continue
        conn= MySQLdb.connect(
            host='172.18.100.5',
            port = 3306,
            user='jobadmin',
            passwd='jobadmin',
            db ='twitter',
            charset = 'utf8'
            )
        cur = conn.cursor()
        cur.execute("select * from twitter_info where id=%s",(new_tweets[i].id,))
        results = cur.fetchone()
        cur.close()
        conn.close()
        if results:
            continue
        save_tweet(new_tweets[i])
def get_all_tweets(user_name,api):
    while True:
        try:
            new_tweets=api.user_timeline(screen_name=user_name,count=200)
            break
        except tweepy.TweepError, e:
            if "Not authorized." in str(e):
                return
            if "Sorry, that page does not exist." in str(e):
                return
            if "Rate limit exceeded" in str(e):
                rate_limit_status=api.rate_limit_status()
                endpoint_time=rate_limit_status['resources']['statuses']['/statuses/user_timeline']['reset']-int(time.time())
                if endpoint_time>0:
                    print 'Rate limit exceeded when get user_timeline(screen_name='+user_name+',count=200):\n'+'sleep '+str(endpoint_time)+' s'
                    time.sleep(endpoint_time)
                else:
                    print 'Rate limit exceeded when get user_timeline(screen_name='+user_name+',count=200):\n'+'sleep 10 s'
                    time.sleep(10)
                continue
            print 'TweepError when get user_timeline(screen_name='+user_name+',count=200):\n'+str(e)
            time.sleep(10)
    while (len(new_tweets) > 0):
        #save_tweets(user_name,new_tweets)
        creat_threads(new_tweets)
        oldest = new_tweets[-1].id - 1
        while True:
            try:
                new_tweets = api.user_timeline(screen_name=user_name,count=200, max_id=oldest)
                break
            except tweepy.TweepError, e:
                if "Not authorized." in str(e):
                    return
                if "Sorry, that page does not exist." in str(e):
                    return
                if "Rate limit exceeded" in str(e):
                    rate_limit_status=api.rate_limit_status()
                    endpoint_time=rate_limit_status['resources']['statuses']['/statuses/user_timeline']['reset']-int(time.time())
                    if endpoint_time>0:
                        print 'Rate limit exceeded when get user_timeline(screen_name='+user_name+',count=200):\n'+'sleep '+str(endpoint_time)+' s'
                        time.sleep(endpoint_time)
                    else:
                        print 'Rate limit exceeded when get user_timeline(screen_name='+user_name+',count=200):\n'+'sleep 10 s'
                        time.sleep(10)
                    continue
                print 'TweepError when get user_timeline(screen_name='+user_name+',count=200,max_id='+str(oldest)+'):\n'+str(e)
                time.sleep(10)

if __name__ == '__main__':
    #在https://apps.twitter.com/app/new注册app应用，获取以下四个认证参数
    consumer_key = "spubu1SRa9fcPoA388aDqWWQl"
    consumer_secret = "J6mGqBm69IwTleOvFIxpEeYy3vV00dONSwm7tA8OHiCcm86y2s"
    access_key = "839320943520428033-TOEBqvX8PdhryGwsxYHaQHQUWunS1vr"
    access_secret = "k20lcTkurhtPNlbwo6nEAdYnB9G1ycYzOYdpnp1qiycqS"
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_key,access_secret)
    api=tweepy.API(auth)
    api.proxy=proxies
    while True:
        user_list=get_users()
        for user in user_list:
            start=time.time()
            total[0]=0
            get_all_tweets(user,api)
            end=time.time()
            print 'use time:'+str(end-start)+' s'
