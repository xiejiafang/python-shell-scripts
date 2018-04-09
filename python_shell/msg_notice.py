#!/usr/bin/python
#coding:utf-8
import commands
import sys
import pyodbc
import datetime
import suds
reload(sys)
sys.setdefaultencoding('utf-8')

#设置数据库连接
conn=pyodbc.connect('DRIVER={DB2};SERVER=192.168.1.1;DATABASE=HNII;UID=db2inst;mypasswd&;charset=utf-8')
cur = conn.cursor()

#web service interface
url='http://10.19.1.16:9090/services/UcstarWebservice?WSDL'
client = suds.client.Client(url)

#获取消息模版
curr_sql= """
SELECT
  a.user,
  sendtime
  FROM db_33.custom_msg a
    WHERE time(commit_time) BETWEEN CURRENT time - 5 MINUTES AND CURRENT time
    and date(commit_time)=current date
"""
result = [result for result in cur.execute(curr_sql.decode('utf-8'))] 
cur.commit()			
for row in result:
    phone_url='http://125.46.83.234:8080'
    oa=row[0]
    msg="您成功定制了一条推送消息，该消息将于每天的%s发送到您的微信!" % row[1]
    sql="insert into administrator.hn_send_message_wx(oa,message,url)values('%s','%s','%s')" % (oa,msg,phone_url)
    cur.execute(sql.decode('utf-8'))
    cur.commit()			
cur.close()

	
