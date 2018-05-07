# -*- coding:utf-8 -*-
"""
python mysql connector
"""
import MySQLdb


def connect(host='218.17.171.90', user='dingzhiwen', passwd='Dzw@123456', db='siat_iot', charset='utf8'):
    db = MySQLdb.connect(host, user=user, passwd=passwd, db=db, charset=charset)
    return db


def connect2(host='210.75.252.89', user='iot_root', passwd='123456', db='iot', charset='utf8'):
    db = MySQLdb.connect(host, user=user, passwd=passwd, db=db, charset=charset)
    return db
