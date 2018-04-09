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
  msg,
  SQL,
  a.category,
  b.category,
  a.branch,
  b.branch,
  a.aracde,
  b.aracde,
  b.user,
  report_id,
  c.name
  FROM db_33.msg_info a,db_33.custom_msg b,db_33.report_list c
    WHERE a.id=b.msg_id
    and (b.status='1' or b.status is null)
    and a.report_id=c.id
    AND sendtime BETWEEN CURRENT time - 5 MINUTES AND CURRENT time
"""
result = [result for result in cur.execute(curr_sql.decode('utf-8'))] 
cur.commit()			
for row in result:
    #组合SQL
    msg = (row[5] or '') + (row[7] or '') + (row[3] or '') + row[0]
    if row[2]:
        category=" and category='%s'" % row[3]
    else:
        category=""
    if (row[4] and row[5] != '分公司'):
        branch=" and branch='%s'" % row[5]
    else:
        branch=""
    if row[6]:
        aracde=" and aracde='%s'" % row[7]
    else:
        aracde=""
    sql=row[1]+category+branch+aracde
    oa=row[8]
    phone_url='http://125.46.83.234:8080'
    pc_url='http://10.19.19.32:8080/init/default/%s/%s' % (row[10],row[9])
    print(sql)
    #执行SQL
    rows = [rows for rows in cur.execute(sql.decode('utf-8'))]
    for row in rows:
        if msg.count('%s')==len(row):
            msg = msg % tuple(row)
	    #wx
	    phone_msg = msg+"\\n感谢使用河南分公司信息定制交互平台!"
            sql="insert into administrator.hn_send_message_wx(oa,message,url)values('%s','%s','%s')" % (oa,phone_msg,phone_url)
            cur.execute(sql.decode('utf-8'))
            cur.commit()			
	    #uc
	    msg = msg.replace('\\n','')
	    pc_msg = msg+"  更多:%s" % pc_url
            client.service.sendMsg(oa,'tk_5800000',pc_msg)
	    print(msg)
	else:
	    print('消息模版的参数与SQL语句的列数不一致!')
	    print(msg)
cur.close()


	
