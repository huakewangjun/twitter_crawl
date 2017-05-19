# -*- coding:utf-8 -*-
import time
import tweepy
import HTMLParser
from tweepy import OAuthHandler
import MySQLdb
import re
import urllib2
import lxml
from bs4 import BeautifulSoup
import json
from save_poc_file_info import *
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
#目前使用的代理端口为8888，可以根据需要进行修改
proxies = {
    'http': 'http://127.0.0.1:1080',
    'https': 'http://127.0.0.1:1080',
}
proxy_handler=urllib2.ProxyHandler(proxies)
opener=urllib2.build_opener(proxy_handler)
urllib2.install_opener(opener)
conn= MySQLdb.connect(
    host='10.10.20.153',
    port = 3306,
    user='root',
    passwd='mima1234',
    db ='twitter',
    charset = 'utf8'
    )
html_parser = HTMLParser.HTMLParser()
total=[0]
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
    link=[]
    if tags_b:
        tags_list=[]
        for b in tags_b:
            tags_list.append(b.get_text().encode("UTF-8", errors='ignore'))
            tags=tags_list
    replace_a=select_content.find_all("a",href=re.compile("https?://t.co/\w+"))
    for a in replace_a:
        if 'data-expanded-url' in a.attrs.keys():
            a.replaceWith(' '+a['data-expanded-url']+" ")
            link.append(a['data-expanded-url'].strip())
        else:
            a.replaceWith(' ')
    if not link:
        link=None
    return select_content.get_text().strip(),tags,link
def save_tweets(keyword,new_tweets):
    for i in range(0,len(new_tweets)):
        if not (new_tweets[i].lang is None or new_tweets[i].lang == "en" or new_tweets[i].lang == "zh"):
            continue
        tweet_id=new_tweets[i].id
        user_name=new_tweets[i].user.screen_name
        href='https://twitter.com/'+user_name+'/status/'+str(tweet_id)
        created_at=new_tweets[i].created_at
        favorite_count=new_tweets[i].favorite_count
        retweet_count=new_tweets[i].retweet_count
        in_reply_to_status_id=new_tweets[i].in_reply_to_status_id
        if hasattr(new_tweets[i],"retweeted_status"):
            retweeted_status_id=new_tweets[i].retweeted_status.id
            retweeted_status_user=new_tweets[i].retweeted_status.user.screen_name.encode("UTF-8", errors='ignore')
            retweeted_status_created_at=new_tweets[i].retweeted_status.created_at
            retweeted_reply_to_status_id=new_tweets[i].retweeted_status.in_reply_to_status_id
            favorite_count=new_tweets[i].retweeted_status.favorite_count
            retweet_count=new_tweets[i].retweeted_status.retweet_count

            retweeted_status_user_id=new_tweets[i].retweeted_status.user.id
            retweeted_status_user_nickname=html_parser.unescape(filter_emoji(new_tweets[i].retweeted_status.user.name)).encode("UTF-8", errors='ignore')
            retweeted_status_user_time_zone=new_tweets[i].retweeted_status.user.time_zone
            if retweeted_status_user_time_zone:
                retweeted_status_user_time_zone=retweeted_status_user_time_zone.encode('utf8',errors='ignore')
            retweeted_status_user_utc_offset=new_tweets[i].retweeted_status.user.utc_offset       
            retweeted_status_user_created_at=new_tweets[i].retweeted_status.user.created_at  
            retweeted_status_user_description=new_tweets[i].retweeted_status.user.description
            if retweeted_status_user_description:
                retweeted_status_user_description=html_parser.unescape(filter_emoji(retweeted_status_user_description)).encode("UTF-8", errors='ignore')
            retweeted_status_user_location= new_tweets[i].retweeted_status.user.location
            if retweeted_status_user_location:
                retweeted_status_user_location=html_parser.unescape(filter_emoji(retweeted_status_user_location)).encode("UTF-8", errors='ignore')              
            retweeted_status_user_statuses_count=new_tweets[i].retweeted_status.user.statuses_count
            retweeted_status_user_friends_count=new_tweets[i].retweeted_status.user.friends_count
            retweeted_status_user_followers_count=new_tweets[i].retweeted_status.user.followers_count
            retweeted_status_user_favourites_count=new_tweets[i].retweeted_status.user.favourites_count
            retweeted_status_user_verified=new_tweets[i].retweeted_status.user.verified
        else:
            retweeted_status_id=None
            retweeted_status_user=None
            retweeted_reply_to_status_id=None
        if hasattr(new_tweets[i],"quoted_status_id"):
            quoted_status_id=new_tweets[i].quoted_status_id
        else:
            quoted_status_id=None
        if True:
            while True:
                try:
                    content,tags,link=get_content_from_html(href)
                    content=html_parser.unescape(filter_emoji(content)).encode("UTF-8", errors='ignore')
                    break
                except Exception as e:
                    print "error in downloading twitter content from "+href,e
                    time.sleep(10)
        focus=retweet_count+favorite_count
        param=(tweet_id,user_name,content,str(link),str(tags),created_at,in_reply_to_status_id,quoted_status_id,retweeted_status_id,retweeted_status_user,retweeted_status_created_at,retweet_count,favorite_count,focus,keyword)
        if link:
            if retweeted_status_id:
                save_poc_file_info(retweeted_status_id,content,link)
            else:
                save_poc_file_info(tweet_id,content,link)
        #将转发的twitter的用户加入用户数据库表
        if retweeted_status_user:
            try:
                param2=(retweeted_status_user,retweeted_status_user_id,retweeted_status_user_nickname,retweeted_status_user_time_zone,retweeted_status_user_utc_offset,\
                        retweeted_status_user_created_at,retweeted_status_user_description,retweeted_status_user_location,retweeted_status_user_statuses_count,\
                        retweeted_status_user_friends_count,retweeted_status_user_followers_count,retweeted_status_user_favourites_count,retweeted_status_user_verified,\
                        retweeted_status_user_nickname,retweeted_status_user_time_zone,retweeted_status_user_utc_offset,\
                        retweeted_status_user_description,retweeted_status_user_location,retweeted_status_user_statuses_count,\
                        retweeted_status_user_friends_count,retweeted_status_user_followers_count,retweeted_status_user_favourites_count,retweeted_status_user_verified)
                cur = conn.cursor()
                cur.execute("insert into twitter_users"+\
                            "(user,count,user_id,nickname,time_zone,utc_offset,created_at,description,location,statuses_count,friends_count,followers_count,favourites_count,verified)"+\
                            " values(%s,1,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "+\
                            "on duplicate key update "+\
                            "nickname=%s,time_zone=%s,utc_offset=%s,description=%s,location=%s,statuses_count=%s,friends_count=%s,followers_count=%s,favourites_count=%s,verified=%s",param2)
                conn.commit()
                cur.close()
                print "insert retweet user ",retweeted_status_user
            except Exception as e:
                print "insert retweet user error when user_name="+retweeted_status_user+',params='+str(param2)+":\n",e
                #sys.exit(1)
        try:
            cur = conn.cursor()
            cur.execute("insert ignore into twitter_info_keyword_poc"+\
                        "(id,screen_name,content,link,tags,time,in_reply_to_status_id,quoted_status_id,retweeted_status_id,retweeted_status_user,"+\
                        "retweeted_status_created_at,retweet_count,favorite_count,focus,keyword) "+\
                        "values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",param)
            conn.commit()
            cur.close()
        except Exception as e:
            print "insert twitter_info error when user_name="+user_name+',params='+str(param)+":\n",e
            #sys.exit(1)
        total[0]=total[0]+1
        print total[0],tweet_id,user_name
def get_tweets_by_keyword(keyword,api_list):
    cur = conn.cursor()
    cur.execute("select max(id),min(id) from twitter_info_keyword_poc where keyword=%s",(keyword,))
    results = cur.fetchone()
    cur.close()
    if not results[0]:
        while True:
            try:
                api=api_list[api_num[0]]
                new_tweets=api.search(q=keyword,count=100,result_type='recent')
                api_num[0]=(api_num[0]+1)%len(api_list)
                break
            except tweepy.TweepError, e:
                print 'TweepError when get api.search(q='+keyword+',count=100):\n',e
                time.sleep(10)
        while (len(new_tweets) > 0):
            save_tweets(keyword,new_tweets)
            oldest = new_tweets[-1].id - 1
            while True:
                try:
                    api=api_list[api_num[0]]
                    new_tweets = api.search(q=keyword,count=100, max_id=oldest,result_type='recent')
                    api_num[0]=(api_num[0]+1)%len(api_list)
                    break
                except tweepy.TweepError, e:
                    print 'TweepError when get api.search(q='+keyword+',count=100,max_id='+str(oldest)+'):\n',e
                    time.sleep(10)
    else:
        max_tweet_id=results[0]
        min_tweet_id=results[1]
        cur = conn.cursor()
        cur.execute("select count(*) from twitter_info_keyword_poc where keyword=%s",(keyword,))
        results = cur.fetchone()
        cur.close()
        total[0]=results[0]
        while True:
            try:
                api=api_list[api_num[0]]
                new_tweets = api.search(q=keyword,count=100, since_id=max_tweet_id,result_type='recent')
                api_num[0]=(api_num[0]+1)%len(api_list)
                break
            except tweepy.TweepError, e:
                print 'TweepError when get search(q='+keyword+',count=100,since_id='+str(max_tweet_id)+'):\n',e
                time.sleep(5)
        while len(new_tweets)==100:
            oldest=new_tweets[-1].id-1
            save_tweets(keyword,new_tweets)
            while True:
                try:
                    api=api_list[api_num[0]]
                    new_tweets = api.search(q=keyword,count=100, max_id=oldest,since_id=max_tweet_id,result_type='recent')
                    api_num[0]=(api_num[0]+1)%len(api_list)
                    break
                except tweepy.TweepError, e:
                    print 'TweepError when get search(q='+keyword+',count=100,since_id='+str(oldest)+'):\n',e
                    time.sleep(5)
        if new_tweets:
            save_tweets(keyword,new_tweets)
        oldest=min_tweet_id-1
        while True:
            while True:
                try:
                    api=api_list[api_num[0]]
                    new_tweets = api.search(q=keyword,count=100, max_id=oldest,result_type='recent')
                    api_num[0]=(api_num[0]+1)%len(api_list)
                    break
                except tweepy.TweepError, e:
                    print 'TweepError when get search(q='+keyword+',count=100,max_id='+str(oldest)+'):\n',e
                    time.sleep(5)
            if new_tweets:
                save_tweets(keyword,new_tweets)
                oldest=new_tweets[-1].id - 1
            else:
                break
def get_api_list():
    with open('ReservedAccount.json') as json_file:
        data = json.load(json_file)
    api_list=[]
    for item in data:
        #在https://apps.twitter.com/app/new注册app应用，获取以下四个认证参数
        consumer_key = item['consumer_key']
        consumer_secret = item['consumer_secret']
        access_key = item['access_key']
        access_secret = item['access_secret']
        auth = OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_key,access_secret)
        api=tweepy.API(auth)
        api.proxy=proxies
        api_list.append(api)
    return api_list
if __name__ == '__main__':
    api_num=[0]
    while True:
        keyword="poc cve"
        api_list=get_api_list()
        get_tweets_by_keyword(keyword,api_list)
        time.sleep(60)