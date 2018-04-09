#!/usr/bin/python
#coding:utf-8
import datetime
import suds
import pyodbc
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

#设置数据库连接
conn=pyodbc.connect('DRIVER={DB2};SERVER=192.168.1.1;DATABASE=hnii;UID=db2inst;mypasswd&;charset=utf-8')
cur = conn.cursor()

#web service interface
url='http://10.19.1.16:9090/services/UcstarWebservice?WSDL'
client = suds.client.Client(url)

#提示内容
sql = """
	SELECT
	hostlog
	FROM db_33.host_log
	WHERE flag='0'
	and alert_type='rsync'
"""
content = cur.execute(sql.decode('utf-8'))
message = [con for con in content]
userlist = ['zrx','luopeng','xiejiafang','chenpeng','caiweizz','botc','lvkun01']
for con in message:
  for user in userlist:
      #uc提示
      print(con[0])
      client.service.sendMsg(user,'tk_5800000',con[0])
      #微信提示
      sql="insert into administrator.hn_send_message_wx(oa,message,url)values('%s','%s','%s')" % (user,con[0],'-')
      cur.execute(sql.decode('utf-8'))
      cur.commit()			
#更新标志
sql = """ update db_33.host_log set flag='1' where flag='0' and alert_type='rsync' """
cur.execute(sql.decode('utf-8'))
cur.commit()			
cur.close()
