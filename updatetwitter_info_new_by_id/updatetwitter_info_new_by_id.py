# -*- coding:utf-8 -*-
import datetime
import time
import tweepy
from tweepy import OAuthHandler
import MySQLdb
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
#目前使用的代理端口为8888，可以根据需要进行修改
proxies = {
    'http': 'http://127.0.0.1:8118',
    'https': 'http://127.0.0.1:8118',
}
conn= MySQLdb.connect(
    host='172.18.100.5',
    port = 3306,
    user='jobadmin',
    passwd='jobadmin',
    db ='twitter',
    charset = 'utf8'
    )
#在https://apps.twitter.com/app/new注册app应用，获取以下四个认证参数
consumer_key = "spubu1SRa9fcPoA388aDqWWQl"
consumer_secret = "J6mGqBm69IwTleOvFIxpEeYy3vV00dONSwm7tA8OHiCcm86y2s"
access_key = "839320943520428033-TOEBqvX8PdhryGwsxYHaQHQUWunS1vr"
access_secret = "k20lcTkurhtPNlbwo6nEAdYnB9G1ycYzOYdpnp1qiycqS"
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_key,access_secret)
api=tweepy.API(auth)
api.proxy=proxies
total=[0]
def get_twitter_id_list():
    cur = conn.cursor()
    cur.execute("select id,time from twitter_info_new")
    results = cur.fetchall()
    twitter_id_list=[]
    twitter_info={}
    for row in results:
        id=row[0]
        created_at=row[1]
        twitter_id_list.append(id)
        twitter_info[id]=created_at
    cur.close()
    return twitter_id_list,twitter_info
def update_tweets(twitter_id):
    while True:
        try:
            new_tweet=api.get_status(twitter_id)
            break
        except tweepy.TweepError, e:
            print 'TweepError when get get_status('+str(twitter_id)+'):\n',e
            if "No status found with that ID." in str(e):
                try:
                    cur = conn.cursor()
                    cur.execute("delete from twitter_info_new where id = %s",(twitter_id,))
                    conn.commit()
                    cur.close()
                except Exception as e:
                    print "delete from twitter_info_new error when id = "+str(twitter_id)+":\n",e                
                return
            time.sleep(10)
    tweet_id=new_tweet.id
    if tweet_id in twitter_id_list:
        favorite_count=new_tweet.favorite_count
        retweet_count=new_tweet.retweet_count
        if hasattr(new_tweet,"retweeted_status"):
            favorite_count=new_tweet.retweeted_status.favorite_count
            retweet_count=new_tweet.retweeted_status.retweet_count
        focus=favorite_count+retweet_count
        try:
            cur = conn.cursor()
            cur.execute("update twitter_info_new set retweet_count=%s,favorite_count=%s,focus=%s where id = %s",(retweet_count,favorite_count,focus,tweet_id))
            conn.commit()
            cur.close()
        except Exception as e:
            print "update twitter_info_new error when user_name="+user_name+',params='+str((retweet_count,favorite_count,focus,tweet_id))+":\n",e
        total[0]=total[0]+1
        print total[0],tweet_id,"update"
if __name__ == '__main__':
    start = time.time()
    utc_time=datetime.datetime.utcnow()
    utc_time_end=utc_time.replace(hour=4,minute=0,second=0, microsecond=0)
    delta1 = datetime.timedelta(days = 1)
    utc_time_start=utc_time_end-delta1
    time_range=[utc_time_start,utc_time_end]
    twitter_id_list,twitter_info=get_twitter_id_list()
    for twitter_id in twitter_id_list:
        update_tweets(twitter_id)
    end = time.time()
    print "use time: ",end-start," s"
