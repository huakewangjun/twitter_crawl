#!/usr/bin/python
# -*- coding:utf-8 -*-
import MySQLdb
import datetime
import smtplib
import re
import ssdeep
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
cur.execute("select tags from twitter_info_new where unix_timestamp(time) >=unix_timestamp(%s) and link is not null and tags is not null and relevancy>=1000 order by focus desc",(utc_time_start,))
results = cur.fetchall()
tags_list=[]
for row in results:
    tags_string=row[0]
    tags_list1=re.findall(""""\S+"|'\S+'""",tags_string)
    for item in tags_list1:
        item=item.strip()[1:-1]
        tags_list.append(item.lower())
counter=collections.Counter(tags_list)
for k in counter:
    word=k
    count=counter[k]
    cur.execute("select * from keywords_vulnerability_test where keyword=%s",(word,))
    result = cur.fetchone()
    if result:
        cur.execute("update keywords_vulnerability_test set count=count+%s where keyword=%s",(count,word))
    else:
        cur.execute("insert into keywords_vulnerability_test(keyword,type,count) values(%s,%s,%s)",(word,'twitter_tag',100))
    conn.commit()

tags_list=[k[0] for k in counter.most_common(10)]

string+="today's top 10 tags:\n"
for item in tags_list:
    string+=item+","
string=string[:-1]+"\n\n"


cur.execute("select * from twitter_info_new where unix_timestamp(time) >=unix_timestamp(%s) and link is not null and relevancy>=1000 order by focus desc",(utc_time_start,))
results = cur.fetchall()
i=0
result_list={}
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
    predict=row[20]
    if not predict:
        predict=0
    hash1 = ssdeep.hash(content.encode("UTF-8",errors='ignore').replace("\n"," "))
    DuplicateContentFlag=False
    for deephashvalue in result_list.values():
        if ssdeep.compare(hash1, deephashvalue)>=3:
            DuplicateContentFlag=True         
            break
    if DuplicateContentFlag:
        continue
    if retweeted_status_id:
        if retweeted_status_id in result_list.keys():
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
        result_list[retweeted_status_id]=hash1
        s=nickname+"@"+retweeted_status_user+'    '+str(create_time+datetime.timedelta(hours=8))+'(GMT+8)'+"\n"+content.encode("UTF-8",errors='ignore').replace("\n"," ")+"\n"+href+"\n"+'retweet_count:'+str(retweet_count)+'    favorite_count:'+str(favorite_count)+'    relevancy:'+str(predict)+'    focus:'+str(focus)+'\n'
        result_info.append(s)
        result_info.append(predict)
        results_info.append(result_info)
    elif id in result_list.keys():
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
        result_list[id]=hash1
        s=nickname+"@"+screen_name+'    '+str(create_time+datetime.timedelta(hours=8))+'(GMT+8)'+"\n"+content.encode("UTF-8",errors='ignore').replace("\n"," ")+"\n"+href+"\n"+'retweet_count:'+str(retweet_count)+'    favorite_count:'+str(favorite_count)+'    relevancy:'+str(predict)+'    focus:'+str(focus)+'\n'
        result_info.append(s)
        result_info.append(predict)
        results_info.append(result_info)

    if len(result_list)>49:
        break
#results_info=mergesort(mergesort(results_info))
#results_info.reverse()
for i in range(len(results_info)):
    string+=str(i+1)+". "+results_info[i][0]+"\n"
print string

send_flag=False
mail_host="smtp.qq.com"
mail_user="wangjun"
mail_pass="kivvertzhmszdihg"
sender = '2439456082@qq.com'
receivers = ['2439456082@qq.com','183403319@qq.com','fengmuyue@iie.ac.cn','wangshiyang@iie.ac.cn','xiaoyang@iie.ac.cn','yuanzimu@iie.ac.cn','huowei@iie.ac.cn','wuwei@iie.ac.cn','wwei@iie.ac.cn','Lijing_Li@bupt.edu.cn']
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
