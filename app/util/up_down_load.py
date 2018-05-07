# -*- coding:utf-8 -*-
"""
upload and download report from server
"""
import urllib

host = '210.75.252.89'
port = '9000'


# def upload(host,port,dir)

def download(host, port, file_name):
    url = 'http://' + host + ':' + port + '/' + file_name
    urllib.urlretrieve(url, file_name)
