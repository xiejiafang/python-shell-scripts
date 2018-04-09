#!/usr/bin/python
#coding:utf-8
import datetime
import suds
import pyodbc
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

#设置数据库连接
conn=pyodbc.connect('DRIVER={DB2};SERVER=10.19.19.34;DATABASE=hnii;UID=db2inst;PWD=okm34db2&;charset=utf-8')
cur = conn.cursor()

#web service interface
url='http://10.19.1.16:9090/services/UcstarWebservice?WSDL'
client = suds.client.Client(url)

#提示内容
sql = """
	SELECT
	'登录提醒:IP '||
	substr(hostlog,LOCATE('from',hostlog)+5,locate('port',hostlog)-LOCATE('from',hostlog)-5)||
	'于 '||substr(hostlog,8,8)||' 尝试使用用户 '||
	substr(hostlog,LOCATE('for',hostlog)+4,locate('from',hostlog)-LOCATE('for',hostlog)-4)||
	'登录'||
	host||
	',登录结果'||
	substr(hostlog,LOCATE(':',hostlog,20),locate('password',hostlog)+locate('publickey',hostlog)-LOCATE(':',hostlog,20))
	FROM db_33.host_log
	WHERE flag='0'
	and alert_type='sshd'
"""
content = cur.execute(sql.decode('utf-8'))
message = [con for con in content]
userlist = ['zrx','luopeng','xiejiafang','chenpeng','caiweizz','botc','lvkun01']
for con in message:
  for user in userlist:
      #uc提示
      client.service.sendMsg(user,'tk_5800000',con[0])
      #微信提示
      sql="insert into administrator.hn_send_message_wx(oa,message,url)values('%s','%s','%s')" % (user,con[0],'-')
      cur.execute(sql.decode('utf-8'))
      cur.commit()			
#更新标志
sql = """ update db_33.host_log set flag='1' where flag='0' """
cur.execute(sql.decode('utf-8'))
cur.commit()			
cur.close()
