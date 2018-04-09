#!/usr/bin/python
#coding:utf-8
import time
import pyodbc

#设置数据库连接
conn=pyodbc.connect('DRIVER={DB2};SERVER=10.19.19.34;DATABASE=HNII;UID=report;PWD=okm123;charset=utf-8')
cur = conn.cursor()

#设置当前时间
currentDate = int(time.strftime("%Y%m%d"))-1

count=1
while count <= 100:
	maxdate = cur.execute("select max(trandate) from administrator.ipe_acct where trandate >decimal(current date -5 days)".decode('utf-8'))
	cur.commit()
	maxdate = maxdate.fetchall()
	maxdate = maxdate[0][0]
	if(maxdate==currentDate):
		print("当日数据已下发")
		print(time.strftime("%H:%M"))
		break
	else:
		print("Try Time:"+str(count))
		count+=1
		time.sleep(60)

