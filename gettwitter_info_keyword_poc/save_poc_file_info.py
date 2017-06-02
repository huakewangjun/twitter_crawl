# -*- coding:utf-8 -*-
import time
import datetime
import os
import lxml
import MySQLdb
import re
import urllib2
import hashlib
import swiftclient
from bs4 import BeautifulSoup
import requests
from pytube import YouTube
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
#下载文件
def download(url, file_name):
    print 'start download file'
    path='d:\\file\\'+file_name
    size=0
    total=0
    finished = False
    print url
    # try:
    #     r = requests.head(url, allow_redirects=True)
    #     try:
    #         total = int(r.headers['Content-Length'])
    #     except Exception as e:
    #         pass
    # except Exception as e:
    #     print "\nDownload error in requests.head.\n"+str(e)
    r = requests.get(url, stream = True)
    with open(path, 'wb') as f:
        try:
            for chunk in r.iter_content(chunk_size = 1024): 
                if chunk:
                    f.write(chunk)
                    size += len(chunk)
                    f.flush()
                #sys.stdout.write('\b' * 64 +'Now: %d, Total: %d' % (size,total))
                sys.stdout.write('\b' * 64 +'Now: %d' % (size,))
                sys.stdout.flush()
            print '\nDownload success'
            finished = True
        except Exception as e:
            print "\nDownload error.\n"+str(e)
    return finished
#获取MD5值
def get_md5(src):
    m2 = hashlib.md5()
    m2.update(src)
    MD5=m2.hexdigest()
    return MD5
#swift是否存在文件
def swift_exist_file(swift_poc_file_name):
    try:
        conn=swiftclient.Connection(user="sf:admin",key="sfadmin",authurl="http://172.18.202.11:8000/auth/v1.0/")
        resp_headers = conn.head_object("SF_NORMAL", swift_poc_file_name)
        #print 'The object was successfully created'
        return 1
    except swiftclient.exceptions.ClientException as e:
        if e.http_status == 404:
            #print 'The object was not found'
            return 0
        else:
            print('An error occurred checking for the existence of the object '+swift_poc_file_name)
            return -1
def swift_upload_file(path,swift_poc_file_name):
    try:
        conn=swiftclient.Connection(user="sf:admin",key="sfadmin",authurl="http://172.18.202.11:8000/auth/v1.0/")
        file_object = open(path,'rb')
        try:
            all_the_text = file_object.read()
        finally:
            file_object.close()
        conn.put_object(
            "SF_NORMAL",
            swift_poc_file_name,
            contents=all_the_text
        )
        return 1
    except Exception,e:
        print 'some exception has appended when put object'+swift_poc_file_name+' into swift\n',e
        return 0
def swift_delete_file(swift_poc_file_name):
    try:
        conn=swiftclient.Connection(user="sf:admin",key="sfadmin",authurl="http://172.18.202.11:8000/auth/v1.0/")
        conn.delete_object(
            "SF_NORMAL",
            swift_poc_file_name
        )
        return 1
    except Exception,e:
        print 'some exception has appended when delete object'+swift_poc_file_name+' from swift\n',e
        return 0
def save_poc_file(html,swift_poc_file_name):
    print 'save poc file'
    existfile=swift_exist_file(swift_poc_file_name)
    if existfile==1:
        print 'The poc file is exist'
        return 1
    elif existfile==0:
        print 'The poc file is not exist'
        pass
    else:
        print 'something wrong when search whether exist file in swift'
        return 0
    path='d:\\file\\'+swift_poc_file_name
    file_object = open(path, 'wb')
    file_object.write(html)
    file_object.close()
    print 'save local poc file success'
    # return 1

    upload_result=swift_upload_file(path,swift_poc_file_name)
    if upload_result:
        #os.remove(path)
        return 1
    else:
        return 0
def download_github_project_file(href, swift_poc_file_name):
    print 'start download github project file'
    i=0
    downloadstatus=False
    while i<3:
        downloadstatus=download(href, swift_poc_file_name)
        if downloadstatus:
            break
        else:
            i=i+1
            print 'download github project fail '+str(i)+' times'
    return downloadstatus
def save_poc_github_project_file(project_user,project_name,branch,root_path,html=None):
    #下载gihub项目根目录网页
    print 'start save poc github project file'
    if not html:
        try:
            r=requests.get(root_path, timeout=10)
            html=r.content
        except Exception as e:
            print 'download github project main html fail from '+root_path
            return 0
    soup = BeautifulSoup(html,'lxml')
    dateModified=soup.select('relative-time')[0]['datetime']
    dateModified=datetime.datetime.strptime(dateModified, "%Y-%m-%dT%H:%M:%SZ")
    commit_sha=soup.select('.commit-tease-sha')[0]['href'].split('/')[-1]
    #查询是否已经存在最新的github项目
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
        cur.execute("select * from poc_github_project_file where user=%s and project=%s and branch=%s",(project_user,project_name,branch))
        result=cur.fetchone()
        cur.close()
    except Exception as e:
        print "select poc_github_project_file error when params="+str((project_user,project_name,branch))+":\n",e
        return 0
    old_swift_project_file_name=None
    if result:
        if result[4]==commit_sha and result[6]==1:
            print 'exist the newest github project'
            return 1
        else:
            old_swift_project_file_name=result[5]
    #下载最新的github项目文件
    select_a=soup.select(".mt-2 a")
    href='https://github.com'+select_a[0]['href']
    project_file_name=project_name+'-'+branch+".zip"
    project_file_name=re.subn('/|\\\\|:|\*|"|\<|\>|\||\?','-',project_file_name)[0]
    swift_project_file_name=get_md5(root_path)+'-'+project_file_name
    download_project_file_status=download_github_project_file(href, swift_project_file_name)
    if not download_project_file_status:
        print 'result:download github project zip fail'
        upload_project_file_status=0
    else:
        #上传最新的github项目文件
        path='d:\\file\\'+swift_project_file_name
        upload_project_file_status=swift_upload_file(path,swift_project_file_name)
        if not upload_project_file_status:
            print 'result:upload github project zip to swift fail'
        # upload_project_file_status=1
    #更新poc_github_project_file数据库表
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
        params=(project_user,project_name,branch,dateModified,commit_sha,swift_project_file_name,upload_project_file_status,dateModified,commit_sha,upload_project_file_status)
        cur.execute("insert into poc_github_project_file(user,project,branch,dateModified,commit_sha,swift_project_file_name,upload_status)\
            values(%s,%s,%s,%s,%s,%s,%s) on duplicate key update dateModified=%s,commit_sha=%s,upload_status=%s"\
            ,params)
        conn.commit()
        cur.close()
    except Exception as e:
        print "insert into poc_github_project_file error when params="+str(params)+":\n",e
        return 0
    # if not old_swift_project_file_name:
    #     #删除过期的github项目文件
    #     delete_status=swift_delete_file(old_swift_project_file_name)
    #     if not delete_status:
    #         print 'delete old_swift_project_file_name fail'
    return upload_project_file_status

def get_github_info(response_url_MD5,response_url,html):
    project_user=response_url.split('/')[3]
    project_name=response_url.split('/')[4]
    soup = BeautifulSoup(html,'lxml')
    select_code=soup.select(".js-file-line")
    if select_code:
        #链接到github中的代码文件
        print 'the href is a code file'
        poc_file_name=response_url.split('/')[-1]
        content=''
        for code in select_code:
            content+=code.get_text()+'\n'
            content=content.strip()
        html=content
        swift_poc_file_name=response_url_MD5+'-'+poc_file_name
        #上传并保存poc文件
        upload_status=save_poc_file(html,swift_poc_file_name)

        root_path=soup.select(".js-path-segment a")[0]['href']
        root_path='https://github.com'+root_path
        branch=soup.select('.selected .select-menu-item-text')[0].get_text().strip()
        save_github_project_file_status=save_poc_github_project_file(project_user,project_name,branch,root_path)

        return upload_status,poc_file_name,swift_poc_file_name,project_user,project_name
    else:
        select_a=soup.select(".mt-2 a")
        if not select_a:
            print 'the href is part of project'
            select_a=soup.select(".js-path-segment a")
            if not select_a:
                #类似于https://github.com/rapid7/metasploit-framework/pull/8499
                root_path='https://github.com'+'/'+project_user+'/'+project_name
                branch='master'
                html=''
            else:
                #类似于https://github.com/rapid7/metasploit-framework/tree/master/app
                root_path=soup.select(".js-path-segment a")[0]['href']
                root_path='https://github.com'+root_path
                branch=soup.select('.selected .select-menu-item-text')[0].get_text().strip()
                html=''
        else:
            print 'the href is root path of th project'
            root_path=response_url
            branch=soup.select('.selected .select-menu-item-text')[0].get_text().strip()
        poc_file_name=project_name+'-'+branch+".zip"
        poc_file_name=re.subn('/|\\\\|:|\*|"|\<|\>|\||\?','-',poc_file_name)[0]
        swift_poc_file_name=get_md5(root_path)+'-'+poc_file_name
        save_github_project_file_status=save_poc_github_project_file(project_user,project_name,branch,root_path,html)
        upload_status=save_github_project_file_status

        return upload_status,poc_file_name,swift_poc_file_name,project_user,project_name
def get_gist_github_info(response_url_MD5,response_url,html):
    project_user=response_url.split('/')[3]
    soup = BeautifulSoup(html,'lxml')
    select_project_name=soup.select(".gist-header-title a")[0].get_text().strip()
    poc_file_name=soup.select(".gist-blob-name")[0].get_text().strip()
    poc_file_name=re.subn('/|\\\\|:|\*|"|\<|\>|\||\?','-',poc_file_name)[0]
    select_code=soup.select(".js-file-line")
    content=''
    for code in select_code:
        content+=code.get_text()+'\n'
        content=content.strip()
    html=content
    swift_poc_file_name=response_url_MD5+'-'+poc_file_name
    upload_status=save_poc_file(html,swift_poc_file_name)
    return upload_status,poc_file_name,swift_poc_file_name,project_user,project_name
def get_youtube_info(response_url_MD5,response_url,html):
    # soup = BeautifulSoup(html,'lxml')
    # poc_file_name=soup.select(".watch-title")[0].get_text().strip()
    # swift_poc_file_name=response_url_MD5+'-'+poc_file_name
    upload_status=0
    poc_file_name=None
    swift_poc_file_name=None
    try:
        yt = YouTube(response_url)
        poc_file_name=yt.filename
        swift_poc_file_name=response_url_MD5+'-'+poc_file_name
        yt.set_filename(swift_poc_file_name)
        print_out = str(yt.filter('mp4')[-1])
        print_out_part = print_out[23:27]
        video = yt.get('mp4', print_out_part)
        video.download('D:\\file')
        download_status=1
    except Exception as e:
        if 'Conflicting filename' in str(e):
            download_status=1
        else:
            print 'download youtube vedio fail:\n',e
            download_status=0
    if download_status:
        poc_file_name=poc_file_name+'.mp4'
        swift_poc_file_name=swift_poc_file_name+'.mp4'
        #判断是否存在poc文件
        existfile=swift_exist_file(swift_poc_file_name)
        if existfile==1:
            return 1,poc_file_name,swift_poc_file_name
        #上传到swift
        path='d:\\file\\'+swift_poc_file_name
        upload_status=swift_upload_file(path,swift_poc_file_name)
        if not upload_status:
            print 'result:upload youtube vedio to swift fail'
    return upload_status,poc_file_name,swift_poc_file_name
def save_poc_file_info(id,content,link):
    #获取content里的cve编号
    content1=re.subn('https?://\S+',' ',content.lower())[0]
    cve_list=re.findall('cve(?:-?|\s*)\d{4}-?\d{4,5}',content1)
    for i in range(len(cve_list)):
        item=cve_list[i]
        number="".join(re.findall('\d',item))
        item='cve-'+number[:4]+'-'+number[4:]
        cve_list[i]=item
    cve_list=list(set(cve_list))
    if not cve_list:
        return
    if len(cve_list)==1:
        cve=cve_list[0]
    else:
        s=''
        for item in cve_list:
            s+=item+','
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
                if response_url.endswith('/'):
                    response_url=response_url[:-1]
                break
            except Exception as e:
                i=i+1
                if i>18:
                    response_url=None
                    break
                print 'try '+str(i)+" times to download twitter href from "+request_url,e
                time.sleep(10)
        project_user=None
        project_name=None
        if not response_url:
            response_url_MD5=None
            domain=None
            poc_file_name=None
            swift_poc_file_name=None
            upload_status=0
        else:
            if r.encoding.lower()!='utf-8':
                html=r.content.decode(r.encoding,"ignore").encode('utf-8',"ignore")
            else:
                html=r.content
            response_url_MD5=get_md5(response_url)
            domain=urllib2.Request(response_url).get_host()
            if domain=='github.com':
                upload_status,poc_file_name,swift_poc_file_name,project_user,project_name=get_github_info(response_url_MD5,response_url,html)
            elif domain=='gist.github.com':
                upload_status,poc_file_name,swift_poc_file_name,project_user,project_name=get_gist_github_info(response_url_MD5,response_url,html)
            elif domain=='www.youtube.com':
                response_url=re.split('\#|&',response_url)[0]
                response_url_MD5=get_md5(response_url)
                upload_status,poc_file_name,swift_poc_file_name=get_youtube_info(response_url_MD5,response_url,html)           
            else:
                soup = BeautifulSoup(html,'lxml')
                poc_file_name=soup.title.string.strip()
                poc_file_name=re.subn('\s+',' ',poc_file_name)[0]
                poc_file_name=re.subn('/|\\\\|:|\*|"|\<|\>|\||\?','-',poc_file_name)[0]+'.html'
                if not poc_file_name:
                    s=re.split('\#|\?|&',response_url)[0]
                    if s.endswith('/'):
                        s=s[:-1]
                    s=s.split('/')[-1]
                    if s.endswith('.htm') or s.endswith('.html'):
                        poc_file_name=s
                    else:
                        poc_file_name=s+'.html'
                swift_poc_file_name=response_url_MD5+'-'+poc_file_name
                upload_status=save_poc_file(html,swift_poc_file_name)
        #保存poc_file信息
        param=(id,content,cve,request_url,request_url_MD5,response_url,response_url_MD5,domain,poc_file_name,swift_poc_file_name,upload_status,project_user,project_name)
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
            cur.execute("insert ignore into poc_file"+\
                        "(id,content,cve,request_url,request_url_MD5,response_url,response_url_MD5,domain,poc_file_name,swift_poc_file_name,upload_status,project_user,project_name) "+\
                        "values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",param)
            conn.commit()
            cur.close()
        except Exception as e:
            print "insert poc_file error when id="+str(id)+',params='+str(param)+":\n",e