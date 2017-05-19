# -*- coding:utf-8 -*-
import time
import os
import lxml
import MySQLdb
import re
import urllib2
import hashlib
from bs4 import BeautifulSoup
import requests
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
proxies = {
    'http': 'http://127.0.0.1:1080',
    'https': 'http://127.0.0.1:1080',
}
proxy_handler=urllib2.ProxyHandler(proxies)
opener=urllib2.build_opener(proxy_handler)
urllib2.install_opener(opener)

def get_md5(src):
    m2 = hashlib.md5()
    m2.update(src)
    MD5=m2.hexdigest()
    return MD5
def save_poc_file(html,swift_poc_file_name):
    path='D:\\file\\'+swift_poc_file_name
    if os.path.exists(path):
        return
    file_object = open(path, 'w')
    file_object.write(html)
    file_object.close()
def save_poc_file_info(id,content,link):
    if not link:
        return
    content1=re.subn('https?://\S+',' ',content.lower())[0]
    cve_list=re.findall('cve-\d{4}-\d{4,5}',content1)
    cve_list1=re.findall('cve\d{8,9}',content1)
    for item in cve_list1:
        item='cve-'+item[3:7]+'-'+item[7:]
        cve_list.append(item)
    cve_list=list(set(cve_list))
    if not cve_list:
        return
    if len(cve_list)==1:
        cve=cve_list[0]
    else:
        s=''
        for item in cve_list:
            s=item+','
        s=s[:-1]
        cve=s
    for item in link:
        i=0
        download_count=0
        request_url=item
        request_url_MD5=get_md5(request_url)
        while 1:
            print 'try to download '+request_url
            try:
                r=requests.get(request_url, timeout=10,proxies=proxies)
                response_url=r.url
                if r.encoding.lower()!='utf-8':
                    html=r.content.decode(r.encoding,"ignore").encode('utf-8',"ignore")
                else:
                    html=r.content
                break
            except Exception as e:
                i=i+1
                if i>18:
                    response_url=None
                    break
                print 'try '+str(i)+" times to download poc file from "+request_url,e
                time.sleep(10)
        if not response_url:
            response_url_MD5=None
            domain_url=None
            poc_file_name=None
            poc_file_MD5=None
        else:
            response_url_MD5=get_md5(response_url)
            domain_url=urllib2.Request(response_url).get_host()
            s=response_url.split('/')[-1]
            s=re.split('\#|\?',s)[0]
            # if domain_url.startswith('https://github.com'):
            #     poc_file_name=s
            if re.match('.*\.\w+',s):
                poc_file_name=s
            else:
                soup = BeautifulSoup(html,'lxml')
                title=soup.title.string.strip()
                title=re.subn('/|\\\\|:|\*|"|\<|\>|\||\?','_',title)[0]
                if title:
                    poc_file_name=title+'.html'
                else:
                    poc_file_name=s+'.html'
            swift_poc_file_name=response_url_MD5+'-'+poc_file_name
            save_poc_file(html,swift_poc_file_name)
        param=(id,content,cve,request_url,request_url_MD5,response_url,response_url_MD5,domain_url,poc_file_name,swift_poc_file_name)
        try:
            conn= MySQLdb.connect(
                host='10.10.20.153',
                port = 3306,
                user='root',
                passwd='mima1234',
                db ='twitter',
                charset = 'utf8'
                )
            cur = conn.cursor()
            cur.execute("insert ignore into poc_file"+\
                        "(id,content,cve,request_url,request_url_MD5,response_url,response_url_MD5,domain_url,poc_file_name,swift_poc_file_name) "+\
                        "values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",param)
            conn.commit()
            cur.close()
        except Exception as e:
            print "insert poc_file error when id="+str(id)+',params='+str(param)+":\n",e









