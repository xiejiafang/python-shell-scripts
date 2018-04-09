#!/usr/bin/python
#coding:utf-8
import commands
import sys
import pyodbc
import os

#设置数据库连接
conn=pyodbc.connect('DRIVER={DB2};SERVER=10.19.19.34;DATABASE=HNII;UID=report;PWD=okm123;charset=utf-8')
cur = conn.cursor()

#清空原有数据
curr_sql= "select tabname from syscat.tables where tabschema='REPORT'"
tables = cur.execute(curr_sql.decode('utf-8')) 

#生成数据
os.system('db2 connect to hnii')
for num in tables:
	#curr_sql='db2 create table test.%s like report.%s' % (num[0],num[0])
	#os.system(curr_sql)
	curr_sql='db2 drop table report.%s' % num[0]
	os.system(curr_sql)
	#curr_sql='db2 create table report.%s like test.%s in tbs_data index in tbs_index' % (num[0],num[0])
	#os.system(curr_sql)
	#curr_sql='db2 drop table test.%s' % num[0]
	#os.system(curr_sql)
	#curr_sql='db2 grant select,update,delete,insert on table report.%s to user report' % num[0]
	#os.system(curr_sql)

cur.commit()			
cur.close()

	
