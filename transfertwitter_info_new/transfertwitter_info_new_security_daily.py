#!/usr/bin/python
# -*- coding:utf-8 -*-
import MySQLdb
import datetime
import re
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

utc_time=datetime.datetime.utcnow()
utc_time_start=utc_time-datetime.timedelta(days=1,hours=1)
conn= MySQLdb.connect(
    host='172.18.100.5',
    port = 3306,
    user='jobadmin',
    passwd='jobadmin',
    db ='twitter',
    charset = 'utf8'
    )
cur = conn.cursor()
cur.execute("select * from twitter_info_new where unix_timestamp(time) >=unix_timestamp(%s) and link is not null and relevancy>=1000",(utc_time_start,))
results = cur.fetchall()
for row in results:
    try:
        params=(row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10],row[11],row[12],row[15],row[11],row[12],row[15])
        cur.execute("insert ignore into twitter_security_daily(id,screen_name,content,link,tags,time,in_reply_to_status_id,quoted_status_id,retweeted_status_id,retweeted_status_user,retweeted_status_created_at,retweet_count,favorite_count,focus) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) on duplicate key update retweet_count=%s,favorite_count=%s,focus=%s",params)
        conn.commit()
        print "transfer success",row[0],row[1]
    except Exception as e:
        print "error in transfer where id="+str(row[0])+"\n",e
