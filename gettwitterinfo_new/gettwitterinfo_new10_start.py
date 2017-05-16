# -*- coding:utf-8 -*-
import os
import subprocess
import sys
reload(sys)
sys.setdefaultencoding('utf-8')


if __name__ == '__main__':
    for i in range(3):
        string='/usr/bin/python ./gettwitterinfo_new10.py '+str(i)
        loader=subprocess.Popen(string, shell=True)
        print 'start gettwitterinfo_new10.py '+str(i)
    print 'start finish'




