#!/usr/bin/python
#coding:utf-8
import commands
import sys
import pyodbc

#设置数据库连接
conn=pyodbc.connect('DRIVER={DB2};SERVER=10.19.19.34;DATABASE=HNII;UID=report;PWD=okm123;charset=utf-8')
cur = conn.cursor()

#清空原有数据
curr_sql= 'delete from administrator.hn_date_list '
cur.execute(curr_sql.decode('utf-8')) 

#生成数据
print("generate data")
for num in range(0,8000):
	curr_sql='insert into administrator.hn_date_list values(decimal(current date - %d days),current date - %d days)' % (num,num)
	cur.execute(curr_sql.decode('utf-8')) 

cur.commit()			
cur.close()

	
