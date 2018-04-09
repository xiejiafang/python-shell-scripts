#!/usr/bin/python
#coding:utf-8
import pyodbc;

#设置数据库连接
conn=pyodbc.connect('DRIVER={DB2};SERVER=10.19.19.229;DATABASE=DJANGO;UID=c.plm;PWD=mypasswd');
cur = conn.cursor();
conn.close();
