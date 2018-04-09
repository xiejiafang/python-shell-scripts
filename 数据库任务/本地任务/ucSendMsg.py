#!/usr/bin/python
#coding:utf-8
import datetime
import suds
import pyodbc
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

#设置数据库连接
conn=pyodbc.connect('DRIVER={DB2};SERVER=192.168.1.1;DATABASE=hnii;UID=report;PWD=mypasswd;charset=utf-8')
cur = conn.cursor()

#web service interface
url='http://10.19.1.16:9090/services/UcstarWebservice?WSDL'
client = suds.client.Client(url)

###########################中支告警###############################
sql = """
select 
  message||'  '||date(eventtime)||' '||time(eventtime)
    From jiankong60.hn_net_events63
      where intime >= current timestamp - 3 minute
"""
print("执行查询")
content = cur.execute(sql)
user_list='wangpf24,hewenchen,xiejiafang,zrx,caiweizz,chenpeng,luopeng,botc,lvkun01,xues03,zhangcy53,guohaitao,houwen,jiajingle,lijiezz,liyulin,macb02,pengmz,qujianwei,wangfp03,chenshaohui,hengcy,sunfei,wudl02,zhangyzk,renchaoyang'
#loop
print("发送消息")
for con in content:
  client.service.sendMsg(user_list,'tk_5800000',con[0])
  print(con[0])
#证照系统不告警
exit

###########################证照系统###############################
#通知管理员
user_list='yangweizz'
sql = """
SELECT
  created_by||'提交了'||branch||license_type||'申请,请处理！'
  FROM db105.zhengzhao_branch_info
    WHERE created_on >= CURRENT timestamp - 5 MINUTES
    and (approval='' or approval is null)
union all
SELECT
  created_by||'提交了'||aracde||license_type||'申请,请处理！'
  FROM db105.zhengzhao_aracde_info
    WHERE created_on >= CURRENT timestamp - 5 MINUTES
    and (approval='' or approval is null)
union all
SELECT
  created_by||'提交了'||zcntcode||license_type||'申请,请处理！'
  FROM db105.zhengzhao_zcntinfo_info
    WHERE created_on >= CURRENT timestamp - 5 MINUTES
    and (approval='' or approval is null)
"""
content = cur.execute(sql.decode('utf-8'))
cur.commit()			
#loop
message = [con for con in content]
for con in message:
  client.service.sendMsg(user_list,'tk_5800000',con[0])
  sql="insert into administrator.hn_send_message_wx(oa,message)values('yangweizz','%s')" % (con[0])
  cur.execute(sql.decode('utf-8'))
  cur.commit()			
  print(con[0])
#通知用户
sql = """
SELECT
  created_by,
  created_by||',您提交的'||branch||license_type||'申请审批结果:'||approval
  FROM db105.zhengzhao_branch_info
    WHERE created_on >= CURRENT timestamp - 5 MINUTES
    and approval<>''
union all
SELECT
  created_by,
  created_by||',您提交的'||aracde||license_type||'申请审批结果:'||approval
  FROM db105.zhengzhao_aracde_info
    WHERE created_on >= CURRENT timestamp - 5 MINUTES
    and approval<>''
union all
SELECT
  created_by,
  created_by||',您提交的'||zcntcode||license_type||'申请审批结果:'||approval
  FROM db105.zhengzhao_zcntinfo_info
    WHERE created_on >= CURRENT timestamp - 5 MINUTES
    and approval<>''
"""
content = cur.execute(sql.decode('utf-8'))
cur.commit()			
#loop
message = [(con[0],con[1]) for con in content]
for con in message:
  client.service.sendMsg(con[0],'tk_5800000',con[1])
  sql="insert into administrator.hn_send_message_wx(oa,message)values('%s','%s')" % (con[0],con[1])
  cur.execute(sql.decode('utf-8'))
  cur.commit()			
  print(con[1])
cur.close()
