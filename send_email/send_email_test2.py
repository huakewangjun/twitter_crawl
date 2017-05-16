#!/usr/bin/python
# -*- coding:utf-8 -*-
import MySQLdb
import datetime
import smtplib
import re
from pattern.en import tag
from email.mime.text import MIMEText
from email.header import Header
import collections
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
def mergesort(seq):  
    if len(seq)<=1:  
        return seq  
    mid=int(len(seq)/2)  
    left=mergesort(seq[:mid])  
    right=mergesort(seq[mid:])  
    return merge(left,right)  
  
def merge(left,right):  
    result=[]  
    i,j=0,0  
    while i<len(left) and j<len(right):  
        if left[i][1]<=right[j][1]:  
            result.append(left[i]) 
            i+=1  
        else:  
            result.append(right[j])  
            j+=1  
    result+=left[i:]
    result+=right[j:]  
    return result

conn= MySQLdb.connect(
    host='172.18.100.5',
    port = 3306,
    user='jobadmin',
    passwd='jobadmin',
    db ='twitter',
    charset = 'utf8'
    )
utc_time=datetime.datetime.utcnow()
utc_time_start=utc_time-datetime.timedelta(days=1)
utc_time_delete_until=utc_time-datetime.timedelta(days=7)
utc_time_end=utc_time
subject="security twitters of "+utc_time.strftime("%Y-%m-%d")+"(use stucco corpus)"
string =subject+"\n\n"
cur = conn.cursor()
cur.execute("select tags from twitter_info_new where unix_timestamp(time) >=unix_timestamp(%s) and link is not null and tags is not null order by focus desc",(utc_time_start,))
results = cur.fetchall()
tags_list=[]
for row in results:
    tags_string=row[0]
    tags_list1=re.findall(""""\S+"|'\S+'""",tags_string)
    for item in tags_list1:
        item=item.strip()[1:-1]
        tags_list.append(item.lower())
counter=collections.Counter(tags_list)
tags_list=[k[0] for k in counter.most_common(10)]

string+="today's top 10 tags:\n"
for item in tags_list:
    string+=item+","
string=string[:-1]+"\n\n"


cur.execute("select * from twitter_info_new where unix_timestamp(time) >=unix_timestamp(%s) and link is not null and statistic_time>300 order by focus desc",(utc_time_start,))
results = cur.fetchall()
i=0
result_list=[]
results_info=[]
for row in results:
    result_info=[]
    id=row[0]
    screen_name=row[1]
    content=row[2]
    retweeted_status_id=row[8]
    retweeted_status_user=row[9]
    retweet_count=row[11]
    favorite_count=row[12]
    focus=row[15]
    text1=re.subn('https?://\S+',' ',content.lower())[0]
    text1=re.subn('@\s*[0-9a-zA-Z-_]+',' ',text1)[0]
    text1=text1.replace('...',' ').strip()
    text1=re.subn('^retweeted\s*[\s\S]*:',' ',text1)[0].strip()
    tag_list=['CD','DT','JJ','JJR','JJS','NN','NNS','NNP','NNPS','NNP-ORG','RB','RBR','RBS','WDT','WP','WP$','WRB','VB','VBZ','VBP','VBD','VBN','VBG']
    wordlist=[word for word, pos in tag(text1) if pos in tag_list]
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
        #predict_list.sort()
        #predict_list.reverse()
        if predict_list:
            for item in predict_list:
                predict+=item
    if predict<5000:
        continue
    if retweeted_status_id:
        if retweeted_status_id in result_list:
            continue
        cur.execute("select nickname from twitter_users where user=%s",(retweeted_status_user,))
        result=cur.fetchone()
        if result:
            nickname = result[0].encode("UTF-8",errors='ignore')
        else:
            nickname = ""
        i=i+1
        create_time=row[10]
        href="https://twitter.com/"+retweeted_status_user+"/status/"+str(retweeted_status_id)
        result_list.append(retweeted_status_id)
        s=nickname+"@"+retweeted_status_user+'    '+str(create_time+datetime.timedelta(hours=8))+'(GMT+8)'+"\n"+content.encode("UTF-8",errors='ignore').replace("\n"," ")+"\n"+href+"\n"+'retweet_count:'+str(retweet_count)+'    favorite_count:'+str(favorite_count)+'    relevancy:'+str(predict)+'    focus:'+str(focus)
        result_info.append(s)
        result_info.append(focus)
        results_info.append(result_info)
    elif id in result_list:
        continue
    else:
        i=i+1
        cur.execute("select nickname from twitter_users where user=%s",(screen_name,))
        result=cur.fetchone()
        if result:
            nickname = result[0].encode("UTF-8",errors='ignore')
        else:
            nickname = ""
        create_time=row[5]
        href="https://twitter.com/"+screen_name+"/status/"+str(id)
        result_list.append(id)
        s=nickname+"@"+screen_name+'    '+str(create_time+datetime.timedelta(hours=8))+'(GMT+8)'+"\n"+content.encode("UTF-8",errors='ignore').replace("\n"," ")+"\n"+href+"\n"+'retweet_count:'+str(retweet_count)+'    favorite_count:'+str(favorite_count)+'    relevancy:'+str(predict)+'    focus:'+str(focus)
        result_info.append(s)
        result_info.append(focus)
        results_info.append(result_info)

    if len(result_list)>49:
        break
results_info=mergesort(mergesort(results_info))
results_info.reverse()
for i in range(len(results_info)):
    string+=str(i+1)+". "+results_info[i][0]+"\n"
print string

send_flag=False
mail_host="smtp.qq.com"
mail_user="wangjun"
mail_pass="kivvertzhmszdihg"
sender = '2439456082@qq.com'
receivers = ['2439456082@qq.com']
message = MIMEText(string, 'plain', 'utf-8')
message['From'] =mail_user+"<"+sender+">"
message['To']=";".join(receivers)
message['Subject'] = Header(subject, 'utf-8') 
try:
    smtpObj = smtplib.SMTP_SSL(mail_host, 465) 
    smtpObj.login(sender,mail_pass)  
    smtpObj.sendmail(sender, receivers, message.as_string())
    print "send receivers success"
    #send_flag=True
except smtplib.SMTPException,e:
    print "Error: send receivers fail"
    print e
if True:
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
            params=(row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[11],row[12],row[2],row[3],row[4],row[11],row[12])
            cur.execute("insert into twitter_info(id,screen_name,content,link,tags,time,in_reply_to_status_id,quoted_status_id,retweeted_status_id,retweeted_status_user,retweet_count,favorite_count) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) on duplicate key update content=%s,link=%s,tags=%s,retweet_count=%s,favorite_count=%s ",params)
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
