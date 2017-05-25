#!/usr/bin/python
# -*- coding:utf-8 -*-
import MySQLdb
import datetime
import re
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

utc_time=datetime.datetime.utcnow()
utc_time_start=utc_time-datetime.timedelta(days=1)
utc_time_delete_until=utc_time-datetime.timedelta(days=7)
utc_time_end=utc_time
conn= MySQLdb.connect(
    host='172.18.100.5',
    port = 3306,
    user='jobadmin',
    passwd='jobadmin',
    db ='twitter',
    charset = 'utf8'
    )
cur = conn.cursor()
cur.execute("select * from twitter_info_new where unix_timestamp(time) <unix_timestamp(%s)",(utc_time_delete_until,))
results = cur.fetchall()
for row in results:
    cur.execute("select timestamp_varas from twitter_info where id=%s",(row[0],))
    result = cur.fetchone()
    if result:
        twitter_info_timestamp_varas=result[0]
        if twitter_info_timestamp_varas>=row[18]:
            try:
                cur.execute("delete from twitter_info_new where id=%s",(row[0],))
                conn.commit()
                print "delete success",row[0],row[1]
            except Exception as e:
                print "error in delete, id="+str(row[0])+"\n",e
            continue
    try:
        params=(row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10],row[11],row[12],row[2],row[3],row[4],row[11],row[12])
        cur.execute("insert into twitter_info(id,screen_name,content,link,tags,time,in_reply_to_status_id,quoted_status_id,retweeted_status_id,retweeted_status_user,retweeted_status_created_at,retweet_count,favorite_count) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) on duplicate key update content=%s,link=%s,tags=%s,retweet_count=%s,favorite_count=%s ",params)
        conn.commit()
        print "transfer success",row[0],row[1]
        try:
            cur.execute("delete from twitter_info_new where id=%s",(row[0],))
            conn.commit()
            print "delete success",row[0],row[1]
        except Exception as e:
            print "error in delete, id="+str(row[0])+"\n",e
    except Exception as e:
        print "error in transfer where id="+str(row[0])+"\n",e
