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

#UC提示，手机报表的建议已有新的回复
sql = """
	SELECT
	username,
        '您的建议:'||COMMENT||'管理员已回复,请查看:http://10.19.19.32:8080/init/default/'||name||'/'||report_id,
        '您对手机报表提出的建议，管理员已回复，请点击查看详细内容，感谢您的支持!',
        'http://125.46.83.234:8080/init/default/'||name||'/'||report_id
	FROM
	db_33.comments a
	LEFT JOIN db_33.replies b ON a.ID = b.comment_id LEFT JOIN db_33.report_list c ON a.report_id=c.id LEFT JOIN db_33.report_user d ON a.user_name=d.first_name
	WHERE comment_id IS NOT NULL
	AND b.commit_time > CURRENT timestamp - 5 minutes
"""
content = cur.execute(sql.decode('utf-8'))
message = [con for con in content]
for con in message:
  client.service.sendMsg(con[0],'tk_5800000',con[1])
  sql="insert into administrator.hn_send_message_wx(oa,message,url)values('%s','%s','%s')" % (con[0],con[2],con[3])
  cur.execute(sql.decode('utf-8'))
  cur.commit()			
  print(con[1])
#提醒管理员有新的建议
sql = """
	SELECT
	username,
        a.user_name||'对报表提出建议,请查看:http://10.19.19.32:8080/init/default/'||name||'/'||report_id,
        a.user_name||'提出了新的建议，请处理!',
        'http://125.46.83.234:8080/init/default/'||name||'/'||report_id
	FROM
	db_33.comments a
	LEFT JOIN db_33.replies b ON a.ID = b.comment_id LEFT JOIN db_33.report_list c ON a.report_id=c.id LEFT JOIN db_33.report_user d ON a.user_name=d.first_name
	WHERE  a.commit_time > CURRENT timestamp - 5 minutes
"""
content = cur.execute(sql.decode('utf-8'))
message = [con for con in content]
admin_list = ['zrx','luopeng','xiejiafang','chenpeng','caiweizz','botc','lvkun01']
for con in message:
    for user in admin_list:
        client.service.sendMsg(user,'tk_5800000',con[1])
        sql="insert into administrator.hn_send_message_wx(oa,message,url)values('%s','%s','%s')" % (user,con[2],con[3])
        cur.execute(sql.decode('utf-8'))
        cur.commit()			
        print(con[1])
cur.close()
