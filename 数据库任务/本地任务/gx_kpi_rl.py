#!/usr/bin/python
#coding:utf-8
import os
import time
import datetime
import pyodbc
import sys
reload(sys)
sys.setdefaultencoding("utf-8")

#设置数据库连接
conn=pyodbc.connect('DRIVER={DB2};SERVER=192.168.1.1;DATABASE=HNII;UID=report;PWD=mypasswd;charset=utf-8')
cur = conn.cursor()

#设置SQL语句

#主管人力
sql_zgrl="""
	delete from administrator.hn_gx_kpi_rl where kpi_name='主管人力' and mon=int(decimal(current date -1 day)/100);
	insert into administrator.hn_gx_kpi_rl
	select branch,aracde,mon,'主管人力',yxzg,szzg,curr_time from v.zgrl_aracde
"""
#月末人力
sql_curr_rl="""
	delete from administrator.hn_gx_kpi_rl where kpi_name='月末人力' and mon={mon};
	insert into administrator.hn_gx_kpi_rl
	select branch,aracde,mon,'月末人力',yx,sz,current timestamp from table(f.mon_end_rl_aracde({date_end})) a
"""
#月初人力
sql_mon_rl="""
	delete from administrator.hn_gx_kpi_rl where kpi_name='月初人力' and mon={mon};
	insert into administrator.hn_gx_kpi_rl
	select branch,aracde,mon,'月初人力',yx,sz,current timestamp from table(f.mon_init_rl_aracde({date_start})) a
"""
#新增人力
sql_add_rl="""
	delete from administrator.hn_gx_kpi_rl where kpi_name='新增人力' and mon={mon};
	insert into administrator.hn_gx_kpi_rl
	select branch,aracde,mon,'新增人力',yx,sz,current_timestamp from table(f.mon_add_rl_aracde({date_start},{date_end})) a
"""
#脱落人力
sql_quit_rl="""
	delete from administrator.hn_gx_kpi_rl where kpi_name='脱落人力' and mon={mon};
	insert into administrator.hn_gx_kpi_rl
	select branch,aracde,mon,'脱落人力',yx,sz,current_timestamp from table(f.mon_quit_rl_aracde({date_start},{date_end})) a
"""
#主管实动
sql_zghd="""
	delete from administrator.hn_gx_kpi_rl where kpi_name='{kpi_name}' and mon={mon};
	insert into administrator.hn_gx_kpi_rl
	select
	branch,
	aracde,
	{mon},
	'{kpi_name}',
	COUNT(CASE WHEN ZGTYPE='YXZG' THEN AGNTNUM END) AS YX,
	COUNT(CASE WHEN ZGTYPE='SZZG' THEN AGNTNUM END) AS SZ,
	current timestamp
	from table(f.gx_hdrl({date_start},{date_end}))
	where zgtype in('YXZG','SZZG')
	and bf>={bf_rule}
	group by branch,aracde,mon
"""
#实动人力
sql_hdrl="""
	delete from administrator.hn_gx_kpi_rl where kpi_name='{kpi_name}' and mon={mon};
	insert into administrator.hn_gx_kpi_rl
	select
	branch,
	aracde,
	{mon},
	'{kpi_name}',
	COUNT(CASE WHEN AGTYPE='YX' THEN AGNTNUM END) AS YX,
	COUNT(CASE WHEN AGTYPE='SZ' THEN AGNTNUM END) AS SZ,
	current timestamp
	from table(f.gx_hdrl({date_start},{date_end}))
	where bf>={bf_rule}
	group by branch,aracde,mon
"""
#新人实动
sql_hdrl_new="""
	delete from administrator.hn_gx_kpi_rl where kpi_name='{kpi_name}' and mon={mon};
	insert into administrator.hn_gx_kpi_rl
	select
	branch,
	aracde,
	{mon},
	'{kpi_name}',
	COUNT(CASE WHEN AGTYPE='YX' THEN AGNTNUM END) AS YX,
	COUNT(CASE WHEN AGTYPE='SZ' THEN AGNTNUM END) AS SZ,
	current timestamp
	from table(f.gx_hdrl(20180101,20180131))
	where bf>={bf_rule}
	and isnew='Y'
	group by branch,aracde,mon
"""

########################################################################################################################
#########数据处理
########################################################################################################################

#设置时间
dateStart = int(time.strftime("%Y%m"))*100+1
dateEnd   = int(time.strftime("%Y%m%d"))
setMon    = int(time.strftime("%Y%m"))
lastDateStart = dateStart - 10000
lastDateEnd = dateEnd - 10000 -1
lastMon = setMon-100
currentHour=int(time.strftime("%H"))
custom_date=datetime.datetime.now()-datetime.timedelta(days=2)


#加工循环次数
if currentHour >= 9:
	count = 1
else:
	count = 2

#主管数据
zghd = [['主管实动','1'],['主管1000P','1000'],['主管3000P','3000'],['主管1万P','10000']]
#实动数据
rlhd = [['实动人力','1'],['1000P人力','1000'],['3000P人力','3000'],['5000P人力','5000'],['1万P人力','10000'],['2万P人力','20000'],['3万P人力','30000'],['5万P人力','50000'],['10万P人力','100000']]
rlhd_new = [['新人实动','1'],['新人3000P','3000']]

#循环
while(count>0):
	#只执行一次
	if (count == 1):
		#主管人力
		for curr_sql in sql_zgrl.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
			cur.commit()			

	#主管实动
	for hd in zghd:
		kpi_name=hd[0]
		rule =hd[1]
		print(kpi_name+":"+str(dateStart)+" "+str(dateEnd)+" "+str(rule))		

		#当年
		currentSQL=sql_zghd.format(
			kpi_name=kpi_name,
			mon=setMon,
			date_start=dateStart,
			date_end=dateEnd,
			bf_rule=rule
		)
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
			cur.commit()			

		#去年
		currentSQL=sql_zghd.format(
			kpi_name=kpi_name,
			mon=lastMon,
			date_start=lastDateStart,
			date_end=lastDateEnd,
			bf_rule=rule
		)
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
			cur.commit()			


	#人力实动
	for hd in rlhd:
		kpi_name=hd[0]
		rule =hd[1]

		#当年
		print(kpi_name+":"+str(dateStart)+" "+str(dateEnd)+" "+str(rule))		
		currentSQL=sql_hdrl.format(
			kpi_name=kpi_name,
			mon=setMon,
			date_start=dateStart,
			date_end=dateEnd,
			bf_rule=rule
		)
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
			cur.commit()			

		#去年
		print(kpi_name+":"+str(lastDateStart)+" "+str(lastDateEnd)+" "+str(rule))		
		currentSQL=sql_hdrl.format(
			kpi_name=kpi_name,
			mon=lastMon,
			date_start=lastDateStart,
			date_end=lastDateEnd,
			bf_rule=rule
		)
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
			cur.commit()			

	#新人实动
	for hd in rlhd_new:
		kpi_name=hd[0]
		rule =hd[1]
		print(kpi_name+":"+str(dateStart)+" "+str(dateEnd)+" "+str(rule))		

		#当年
		currentSQL=sql_hdrl_new.format(
			kpi_name=kpi_name,
			mon=setMon,
			date_start=dateStart,
			date_end=dateEnd,
			bf_rule=rule
		)
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
			cur.commit()			

		#去年
		currentSQL=sql_hdrl_new.format(
			kpi_name=kpi_name,
			mon=lastMon,
			date_start=lastDateStart,
			date_end=lastDateEnd,
			bf_rule=rule
		)
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
			cur.commit()			


	#当天不变动的数据，只在9点前加工
	if currentHour <= 9:
		#月末人力
		print("月末人力")
		#当前
		currentSQL=sql_curr_rl.format(
			mon=setMon,
			date_end=dateEnd,
			)
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
			cur.commit()			
		#同期
		currentSQL=sql_curr_rl.format(
			mon=lastMon,
			date_end=lastDateEnd,
			)
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
			cur.commit()			


		#月初人力
		print("月初人力")
		#当前
		currentSQL=sql_mon_rl.format(
			mon=setMon,
			date_start=dateStart,
			)
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
			cur.commit()			
		#同期
		currentSQL=sql_mon_rl.format(
			mon=lastMon,
			date_start=lastDateStart,
			)
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
			cur.commit()			

		#新增人力
		print("新增人力")
		#当前
		currentSQL=sql_add_rl.format(
			mon=setMon,
			date_start=dateStart,
			date_end=dateEnd,
			)
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
			cur.commit()			
		#同期
		currentSQL=sql_add_rl.format(
			mon=lastMon,
			date_start=lastDateStart,
			date_end=lastDateEnd,
			)
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
			cur.commit()			

		#脱落人力
		print("脱落人力")
		#当前
		currentSQL=sql_quit_rl.format(
			mon=setMon,
			date_start=dateStart,
			date_end=dateEnd,
			)
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
			cur.commit()			
		#同期
		currentSQL=sql_quit_rl.format(
			mon=lastMon,
			date_start=lastDateStart,
			date_end=lastDateEnd,
			)
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
			cur.commit()			

	############################################################################
	#循环-1
	count-=1
	#重置时间
	dateStart = dateStart-100
	dateEnd   = dateStart+31
	setMon    = setMon-1
	lastDateStart = dateStart - 10000
	lastDateEnd = dateEnd - 10000
	lastMon = setMon-100
#关闭连接
cur.close()
