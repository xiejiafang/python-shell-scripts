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
curr_sql= "select tabname from syscat.tables where tabschema='WEB'"
tables = cur.execute(curr_sql.decode('utf-8')) 

os.system('db2 connect to hnii')
#生成数据
for num in tables:
	curr_sql= "db2 \"delete from web.{table}\""
	sql = curr_sql.format(table=num[0])
	os.system(sql)
	curr_sql= "db2 \"insert into web.{table} select * from db_105.{table}\""
	sql = curr_sql.format(table=num[0])
	os.system(sql)
	if num[0]=='report_user':
		curr_sql = "db2 \" delete from db_33.report_user\""
	  	os.system(sql)
		curr_sql = "db2 \" insert into db_33.report_user (first_name,username,password) select  first_name,username,password  from web.report_user\""
	  	os.system(sql)

os.system('db2 connect reset ')
cur.commit()			
cur.close()

	
