# -*- coding:utf-8 -*-
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
#在https://apps.twitter.com/app/new注册app应用，获取以下四个认证参数
consumer_key = "qqDI1zFlo7Gfd9MiPA4iYZEeN"
consumer_secret = "ZOtvc9osq37IPYDtyvq2LgRqQfPtMOJwoUOJsxo97leDJYnL9B"
access_key = "839320943520428033-42y3H4ukVgUl23RJnnLouz2sJpoBWZ5"
access_secret = "YBl2bXBCQaoWmymIAaDAwPViDxZUOXiY3kufVOUCOhmwv"
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_key,access_secret)
api=tweepy.API(auth)
api.proxy=proxies
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
def save_tweets(keyword,new_tweets):
    for i in range(0,len(new_tweets)):
        tweet_id=new_tweets[i].id
        user_name=new_tweets[i].user.screen_name
        href='https://twitter.com/'+user_name+'/status/'+str(tweet_id)
        # content=html_parser.unescape(filter_emoji(new_tweets[i].text)).encode("UTF-8", errors='ignore')
        created_at=new_tweets[i].created_at.strftime("%Y-%m-%d %H:%M:%S")
        favorite_count=new_tweets[i].favorite_count
        retweet_count=new_tweets[i].retweet_count
        in_reply_to_status_id=new_tweets[i].in_reply_to_status_id
        # if new_tweets[i]._json['entities']['hashtags']:
        #     tags_list=[]
        #     for item in new_tweets[i]._json['entities']['hashtags']:
        #         tags_list.append(item['text'].encode("UTF-8", errors='ignore'))
        #     tags=str(tags_list)
        # else:
        #     tags=None
        if hasattr(new_tweets[i],"retweeted_status"):
            retweeted_status_id=new_tweets[i].retweeted_status.id
            # content=html_parser.unescape(filter_emoji(new_tweets[i].retweeted_status.text)).encode("UTF-8", errors='ignore')
            retweeted_status_user=new_tweets[i].retweeted_status.user.screen_name.encode("UTF-8", errors='ignore')
            retweeted_status_created_at=new_tweets[i].retweeted_status.created_at.strftime("%Y-%m-%d %H:%M:%S")
            retweeted_reply_to_status_id=new_tweets[i].retweeted_status.in_reply_to_status_id
            # if new_tweets[i].retweeted_status._json['entities']['hashtags']:
            #     tags_list=[]
            #     for item in new_tweets[i].retweeted_status._json['entities']['hashtags']:
            #         tags_list.append(item['text'].encode("UTF-8", errors='ignore'))
            #     tags=str(tags_list)
            # else:
            #     tags=None
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
            # s=re.findall("https?://t.co/\w+$",content)
            # if s:
            #     content=content[:-len(s[0])].strip()
        else:
            quoted_status_id=None
        #content=filter_emoji(new_tweets[i].text).encode("UTF-8", errors='ignore')
        if True:#(u'\u2026' in content) or (in_reply_to_status_id is not None) or (retweeted_reply_to_status_id is not None):
            while True:
                try:
                    content,tags=get_content_from_html(href)
                    content=html_parser.unescape(filter_emoji(content)).encode("UTF-8", errors='ignore')
                    break
                except Exception as e:
                    print "error in downloading twitter content from "+href,e
                    time.sleep(10)
        if re.findall('https?://\S+\.\S+\w',content):
            link=str(re.findall('https?://\S+\.\S+\w',content))
        else:
            link=None
        focus=retweet_count+favorite_count
        param=(tweet_id,user_name,content,link,tags,created_at,in_reply_to_status_id,quoted_status_id,retweeted_status_id,retweeted_status_user,retweet_count,favorite_count,focus,keyword)
        #将转发的twitter的用户加入用户数据库表
        try:
            cur = conn.cursor()
            cur.execute("insert ignore into twitter_info_keyword_test"+\
                        "(id,screen_name,content,link,tags,time,in_reply_to_status_id,quoted_status_id,retweeted_status_id,retweeted_status_user,"+\
                        "retweet_count,favorite_count,focus,keyword) "+\
                        "values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",param)
            conn.commit()
            cur.close()
        except Exception as e:
            print "insert twitter_info error when user_name="+user_name+',params='+str(param)+":\n",e
            #sys.exit(1)
        total[0]=total[0]+1
        print total[0],tweet_id,user_name
def get_tweets_by_keyword(keyword):
    cur = conn.cursor()
    cur.execute("select max(id),min(id) from twitter_info_keyword where keyword=%s",(keyword,))
    results = cur.fetchone()
    cur.close()
    if not results[0]:
        while True:
            try:
                new_tweets=api.search(q=keyword,count=100,result_type='recent')
                break
            except tweepy.TweepError, e:
                print 'TweepError when get search(q='+keyword+',count=100):\n',e
                time.sleep(10)
        while (len(new_tweets) > 0):
            save_tweets(keyword,new_tweets)
            oldest = new_tweets[-1].id - 1
            while True:
                try:
                    new_tweets = api.search(q=keyword,count=100, max_id=oldest,result_type='recent')
                    break
                except tweepy.TweepError, e:
                    print 'TweepError when get search(q='+keyword+',count=100,max_id='+str(oldest)+'):\n',e
                    time.sleep(10)
    else:
        newest=results[0]
        oldest=results[1]-1
        cur = conn.cursor()
        cur.execute("select count(*) from twitter_info_keyword where keyword=%s",(keyword,))
        results = cur.fetchone()
        cur.close()
        total[0]=results[0]
        while True:
            while True:
                try:
                    new_tweets = api.search(q=keyword,count=100, since_id=newest,result_type='recent')
                    break
                except tweepy.TweepError, e:
                    print 'TweepError when get search(q='+keyword+',count=100,since_id='+str(newest)+'):\n',e
                    time.sleep(10)
            if new_tweets:
                save_tweets(keyword,new_tweets)
                newest=new_tweets[0].id
            else:
                break
        while True:
            while True:
                try:
                    new_tweets = api.search(q=keyword,count=100, max_id=oldest,result_type='recent')
                    break
                except tweepy.TweepError, e:
                    print 'TweepError when get search(q='+keyword+',count=100,max_id='+str(oldest)+'):\n',e
                    time.sleep(10)
            if new_tweets:
                save_tweets(keyword,new_tweets)
                oldest=new_tweets[-1].id - 1
            else:
                break
if __name__ == '__main__':
    while True:
        keyword="#life"
        get_tweets_by_keyword(keyword)
        time.sleep(10)
