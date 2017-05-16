# -*- coding:utf-8 -*-
import gc
import os
import subprocess
import json
import datetime
import time
import threadpool
import threading
import tweepy
import HTMLParser
from tweepy import OAuthHandler
import MySQLdb
import re
import urllib2
from bs4 import BeautifulSoup
from pattern.en import tag
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
html_parser = HTMLParser.HTMLParser()
def get_users(a):
    conn= MySQLdb.connect(
        host='172.18.100.5',
        port = 3306,
        user='jobadmin',
        passwd='jobadmin',
        db ='twitter',
        charset = 'utf8'
        )
    cur = conn.cursor()
    cur.execute("select user from twitter_users order by count desc,user asc limit "+str(a*1000)+",1000")
    results = cur.fetchall()
    user_list=[]
    for row in results:
        user_name=row[0].encode('utf8')
        user_list.append(user_name)
    cur.close()
    return user_list
def get_twitter_id_list(user_list):
    conn= MySQLdb.connect(
        host='172.18.100.5',
        port = 3306,
        user='jobadmin',
        passwd='jobadmin',
        db ='twitter',
        charset = 'utf8'
        )
    cur = conn.cursor()
    cur.execute("select * from twitter_info_new where unix_timestamp(time) >=unix_timestamp(%s)",(datetime.datetime.utcnow()-datetime.timedelta(days=1),))
    results = cur.fetchall()
    twitter_id_list=[]
    twitter_id_info={}
    for row in results:
        if not (row[1] in user_list):
            continue
        info={}
        id=row[0]
        timestamp_varas=row[18]
        timestamp_insert=row[19]
        last_focus=row[13]+row[14]
        info['last_focus']=last_focus
        info['timestamp_insert']=timestamp_insert
        info['timestamp_varas']=timestamp_varas
        twitter_id_list.append(id)
        twitter_id_info[id]=info
    cur.close()
    return twitter_id_list,twitter_id_info
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
def get_relevancy(content):
    conn= MySQLdb.connect(
        host='172.18.100.5',
        port = 3306,
        user='jobadmin',
        passwd='jobadmin',
        db ='twitter',
        charset = 'utf8'
        )
    cur = conn.cursor()
    text1=re.subn('https?://\S+',' ',content.lower())[0]
    text1=re.subn('@\s*[0-9a-zA-Z-_]+',' ',text1)[0]
    text1=text1.replace('...',' ').strip()
    text1=re.subn('^retweeted\s*[\s\S]*:',' ',text1)[0].strip()
    #tag_list=['CD','DT','JJ','JJR','JJS','NN','NNS','NNP','NNPS','NNP-ORG','RB','RBR','RBS','WDT','WP','WP$','WRB','VB','VBZ','VBP','VBD','VBN','VBG']
    wordlist=[word for word, pos in tag(text1)]
    predict_list=[]
    predict=0
    if wordlist:
        for item in wordlist:
            if re.match('cve-\d+-\d+',item):
                item='cve'
            cur.execute("select count from keywords_vulnerability_test where keyword=%s",(item,))
            results3=cur.fetchone()
            if results3:
                predict_list.append(results3[0])
        if predict_list:
            for item in predict_list:
                predict+=item
    return predict
def save_tweets(user_name,new_tweets,threadnum):
    for i in range(0,len(new_tweets)):
        if new_tweets[i].created_at<(datetime.datetime.utcnow()-datetime.timedelta(days=1)):
            return
        if not (new_tweets[i].lang is None or new_tweets[i].lang == "en"):
            continue
        tweet_id=new_tweets[i].id
        created_at=new_tweets[i].created_at
        favorite_count=new_tweets[i].favorite_count
        retweet_count=new_tweets[i].retweet_count
        focus=favorite_count+retweet_count+1
        retweeted_status_created_at=None
        if tweet_id in twitter_id_list:
            if hasattr(new_tweets[i],"retweeted_status"):
                retweeted_status_id=new_tweets[i].retweeted_status.id
                retweeted_status_created_at=new_tweets[i].retweeted_status.created_at
                favorite_count=new_tweets[i].retweeted_status.favorite_count
                retweet_count=new_tweets[i].retweeted_status.retweet_count
                focus=favorite_count+retweet_count+1
                if retweeted_status_created_at<(datetime.datetime.utcnow()-datetime.timedelta(days=1)):
                    focus=focus-twitter_id_info[tweet_id]['last_focus']

            utc_time=datetime.datetime.utcnow()
            now_time=utc_time+datetime.timedelta(hours=8)
            until_time=utc_time-datetime.timedelta(days=1)
            if retweeted_status_created_at and retweeted_status_created_at<until_time:
                statistic_time=int((now_time-twitter_id_info[tweet_id]['timestamp_insert']).total_seconds())
            elif retweeted_status_created_at:
                statistic_time=int((utc_time-retweeted_status_created_at).total_seconds())
            else:
                statistic_time=int((utc_time-created_at).total_seconds())
            if statistic_time==0:
                hot_rate=0
            else:
                hot_rate=focus/float(statistic_time)
            timestamp_varas=now_time
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
                cur.execute("update twitter_info_new set retweet_count=%s,favorite_count=%s,focus=%s,statistic_time=%s,hot_rate=%s,timestamp_varas=%s where id = %s",(retweet_count,favorite_count,focus,statistic_time,hot_rate,timestamp_varas,tweet_id))
                conn.commit()
                cur.close()
                total[0]=total[0]+1
                print 'Thread-'+str(threadnum)+' '+str(total[0])+' '+str(tweet_id)+' '+str(user_name)+' '+"update"
            except Exception as e:
                print "update in twitter_info_new error when user_name="+user_name+',params='+str((retweet_count,favorite_count,focus,statistic_time,hot_rate,timestamp_varas,tweet_id))+":\n"+str(e)
            continue
        href='https://twitter.com/'+user_name+'/status/'+str(tweet_id)
        created_at=new_tweets[i].created_at
        in_reply_to_status_id=new_tweets[i].in_reply_to_status_id
        favorite_count=new_tweets[i].favorite_count
        retweet_count=new_tweets[i].retweet_count
        focus=favorite_count+retweet_count+1
        if hasattr(new_tweets[i],"retweeted_status"):
            retweeted_status_id=new_tweets[i].retweeted_status.id
            retweeted_status_user=new_tweets[i].retweeted_status.user.screen_name.encode("UTF-8", errors='ignore')
            retweeted_status_created_at=new_tweets[i].retweeted_status.created_at
            retweeted_reply_to_status_id=new_tweets[i].retweeted_status.in_reply_to_status_id
            favorite_count=new_tweets[i].retweeted_status.favorite_count
            retweet_count=new_tweets[i].retweeted_status.retweet_count
            focus=favorite_count+retweet_count+1
            if retweeted_status_created_at<(datetime.datetime.utcnow()-datetime.timedelta(days=1)):
                focus=1

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
            retweeted_status_created_at=None
            retweeted_reply_to_status_id=None
        if hasattr(new_tweets[i],"quoted_status_id"):
            quoted_status_id=new_tweets[i].quoted_status_id
        else:
            quoted_status_id=None
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
        relevancy=get_relevancy(content)
        utc_time=datetime.datetime.utcnow()
        now_time=utc_time+datetime.timedelta(hours=8)
        until_time=utc_time-datetime.timedelta(days=1)
        if retweeted_status_created_at and retweeted_status_created_at<until_time:
            statistic_time=0
        elif retweeted_status_created_at:
            statistic_time=int((utc_time-retweeted_status_created_at).total_seconds())
        else:
            statistic_time=int((utc_time-created_at).total_seconds())
        if statistic_time==0:
            hot_rate=0
        else:
            hot_rate=focus/float(statistic_time)
        timestamp_varas=now_time
        timestamp_insert=now_time
        param=(tweet_id,user_name,content,link,tags,created_at,in_reply_to_status_id,quoted_status_id,retweeted_status_id,retweeted_status_user,retweeted_status_created_at,retweet_count,favorite_count,retweet_count,favorite_count,focus,statistic_time,hot_rate,timestamp_varas,timestamp_insert,relevancy)
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
                print 'Thread-'+str(threadnum)+' '+"insert retweet user "+retweeted_status_user
            except Exception as e:
                print "insert retweet user error when user_name="+retweeted_status_user+',params='+str(param2)+":\n"+str(e)
                #sys.exit(1)
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
            cur.execute("insert ignore into twitter_info_new(id,screen_name,content,link,tags,time,in_reply_to_status_id,quoted_status_id,retweeted_status_id,retweeted_status_user,retweeted_status_created_at,retweet_count,favorite_count,insert_retweet_count,insert_favorite_count,focus,statistic_time,hot_rate,timestamp_varas,timestamp_insert,relevancy) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",param)
            conn.commit()
            cur.close()
            total[0]=total[0]+1
            print 'Thread-'+str(threadnum)+' '+str(total[0])+' '+str(tweet_id)+' '+str(user_name)+" insert"
        except Exception as e:
            print "insert into twitter_info_new error when user_name="+user_name+',params='+str(param)+":\n"+str(e)
            #sys.exit(1)

def get_all_tweets(user_name,api,threadnum):
    while True:
        try:
            new_tweets=api.user_timeline(screen_name=user_name,count=100)
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
                    print 'Rate limit exceeded when get user_timeline(screen_name='+user_name+',count=100):\n'+'sleep '+str(endpoint_time)+' s'
                    time.sleep(endpoint_time)
                else:
                    print 'Rate limit exceeded when get user_timeline(screen_name='+user_name+',count=100):\n'+'sleep 10 s'
                    time.sleep(10)
                continue
            print 'TweepError when get user_timeline(screen_name='+user_name+',count=100):\n'+str(e)
            time.sleep(10)
    while (len(new_tweets) > 0):
        if new_tweets[0].created_at<(datetime.datetime.utcnow()-datetime.timedelta(days=1)):
            return
        save_tweets(user_name,new_tweets,threadnum)
        oldest = new_tweets[-1].id - 1
        if new_tweets[-1].created_at<=(datetime.datetime.utcnow()-datetime.timedelta(days=1)):
            return
        while True:
            try:
                new_tweets = api.user_timeline(screen_name=user_name,count=100, max_id=oldest)
                break
            except tweepy.TweepError, e:
                if "Rate limit exceeded" in str(e):
                    rate_limit_status=api.rate_limit_status()
                    endpoint_time=rate_limit_status['resources']['statuses']['/statuses/user_timeline']['reset']-int(time.time())
                    if endpoint_time>0:
                        print 'Rate limit exceeded when get user_timeline(screen_name='+user_name+',count=100):\n'+'sleep '+str(endpoint_time)+' s'
                        time.sleep(endpoint_time)
                    else:
                        print 'Rate limit exceeded when get user_timeline(screen_name='+user_name+',count=100):\n'+'sleep 10 s'
                        time.sleep(10)
                    continue
                print 'TweepError when get user_timeline(screen_name='+user_name+',count=100,max_id='+str(oldest)+'):\n'+str(e)
                time.sleep(10)
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
def create_threadpool(user_list,api_list,threadnum):
    start = time.time()
    request_list=[]
    for i in range(len(user_list)):
        request_list.append(([user_list[i],api_list[i%len(api_list)],threadnum], None))
    pool = threadpool.ThreadPool(10)
    requests = threadpool.makeRequests(get_all_tweets, request_list)
    for req in requests:
        pool.putRequest(req)
    pool.wait()
    end = time.time()
    print 'Thread-'+str(threadnum)+' '+"use time: "+str(end-start)+" s"
if __name__ == '__main__':
    a=sys.argv[1]
    try:
        a=int(a)
    except Exception as e:
        raise e
    utc_time=datetime.datetime.utcnow()
    utc_time_start=utc_time-datetime.timedelta(days=1)
    utc_time_end=utc_time
    time_range=[utc_time_start,utc_time_end]
    user_list=get_users(a)
    twitter_id_list,twitter_id_info=get_twitter_id_list(user_list)
    api_list=get_api_list()
    ls_thread = []
    total=[0]
    create_threadpool(user_list,api_list,a)
    string='/usr/bin/python ./gettwitterinfo_new10.py '+str(a)
    while 1:
        try:
            loader=subprocess.Popen(string, shell=True)
            print 'Thread-'+str(a)+' '+'restart gettwitterinfo_new10.py '+str(a)
            break
        except Exception as e:
            print e


