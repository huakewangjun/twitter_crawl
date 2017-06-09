# -*- coding:utf-8 -*-
import os
import subprocess
import sys
reload(sys)
sys.setdefaultencoding('utf-8')


if __name__ == '__main__':
    string='ps -ef|grep gettwitterinfo_new10.py|grep -v grep|cut -c 9-15|xargs kill -9'
    try:
        loader=subprocess.Popen(string, shell=True)
    except Exception as e:
        pass
    string='python gettwitterinfo_new10.py 0 &'
    loader=subprocess.Popen(string, shell=True)
    print 'restart finish'




