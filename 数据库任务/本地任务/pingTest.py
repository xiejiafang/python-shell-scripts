#!/usr/bin/python
#coding:utf-8
import commands
import sys
import pyodbc

#设置数据库连接
conn=pyodbc.connect('DRIVER={DB2};SERVER=192.168.1.1;DATABASE=HNII;UID=report;PWD=mypasswd;charset=utf-8')
cur = conn.cursor()

ip_array =(
	'192.168.1.1',
	'10.19.19.101',
	'10.19.19.103',
	'10.19.19.104',
	) 

for ip in ip_array:
	(status,output) = commands.getstatusoutput('ping -c3 %s' % ip);	
	if (status == 0):
		print('%s 正常' % ip);
	else:
		curr_sql= 'insert into hnii0.hn_sms_msg(SMSFL,HANDSETNO,SMSINFO,STATCODE,USERID,DS_TIME) select \'datacheck\',handphone,\'警告：%s 故障\',\'0\',\'XIE\',date(current timestamp) from administrator.sms_list where smsfl = \'CHECK\' and handphone=\'18037678556\'' % ip;
		cur.execute(curr_sql.decode('utf-8')) 
		cur.commit()			
		cur.close()

	
