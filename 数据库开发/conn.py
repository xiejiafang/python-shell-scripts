#!/usr/bin/python
#coding:utf-8
import pyodbc;

#设置数据库连接
conn=pyodbc.connect('DRIVER={DB2};SERVER=10.19.19.109;DATABASE=DJANGO;UID=c.plm;PWD=okm123');
cur = conn.cursor();
conn.close();
