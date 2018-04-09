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
#业绩
sql_yj="""
delete from administrator.hn_kpi_info_bnk_2017 where category='{category_zh}' and series='渠道' and kpi_name='{kpi_name}' and kpi_mon={setMon};
insert into administrator.hn_kpi_info_bnk_2017
with mytable as(
SELECT 
        case when series in('SQ','FIC') then 'SQ' when series in('TZ','TZF') then 'TZ' else 'NO' end as sys,
        case when series in('FIC','TZF') then 'FIC' else branch_name end as branch_name,
        flag,
        case when bk_lgcy_id ='' or bk_lgcy_id is null then 'AA' else substr(bk_lgcy_id,3,2) end as bank,
        agntnum,case when series in('FIC','TZF') then 'FIC' else branch end branch,
        chdrnum,
        sum(case when trandate between {thisDateStart} and {thisDateEnd} then {field} else 0 end) as this_bf,
        sum(case when trandate between {lastDateStart} and {lastDateEnd} then {field} else 0 end) as last_bf,
        case when sum(case when trandate between {thisDateStart} and {thisDateEnd} then {field} else 0 end)>0 then 1 
                when sum(case when trandate between {thisDateStart} and {thisDateEnd} then {field} else 0 end)<0 then -1    
                else 0 end as this_js,
        case when sum(case when trandate between {lastDateStart} and {lastDateEnd} then {field} else 0 end)>0 then 1
                when sum(case when trandate between {lastDateStart} and {lastDateEnd} then {field} else 0 end)<0 then -1
                else 0 end as last_js
                FROM V.IPE_ACCT_BNK_KPI
                        where (trandate between {thisDateStart} and {thisDateEnd} or trandate between {lastDateStart} and {lastDateEnd})
                        {andWhere}
                                group by 
                                series,
                                branch_name,branch,
                                flag,
                                case when bk_lgcy_id ='' or bk_lgcy_id is null then 'AA' else substr(bk_lgcy_id,3,2) end,
                                agntnum,
                                chdrnum)
select 
        value(category,'{category_zh}') as category,
        value(medi,'渠道'),
        value(name,''),
        value(flag,'无网点'),
        value(bank,'AA'),
        value(kpi_name,'{kpi_name}'),
        value(kpi_mon,{setMon}),
        value(this_bf,0),
        value(last_bf,0),
        current timestamp 
        from 
        (select branch,name from administrator.hn_branch where branch<>'D' union values('FIC','FIC')) a left join
(select 
        '{category_zh}' as category,
        '渠道' as medi,
        branch,
        flag,
        bank,
        '{kpi_name}' as kpi_name,
	{setMon} as kpi_mon,
        {operate_field1} as this_bf,
        {operate_field2} as last_bf
                from mytable  
			where sys='{sys}'
                        group by 
                                branch,
                                flag,
                                bank) b on a.branch=b.branch
"""
#人力
sql_rl="""
delete from administrator.hn_kpi_info_bnk_2017 where category='{category_zh}' and series='渠道' and kpi_name='{kpi_name}' and kpi_mon={setMon};
insert into administrator.hn_kpi_info_bnk_2017
with mytable as(
SELECT 
        case when series in('SQ','FIC') then 'SQ' else 'TZ' end as sys,
        case when series in('FIC','TZF') then 'FIC' else branch_name end as branch_name,
        'flag' as flag,
        agntnum,
        sum(case when trandate between {thisDateStart} and {thisDateEnd} then {field} else 0 end) as this_bf,
        sum(case when trandate between {lastDateStart} and {lastDateEnd} then {field} else 0 end) as last_bf,
        count(distinct case when trandate between {thisDateStart} and {thisDateEnd} and ape>0 then chdrnum end)-
        count(distinct case when trandate between {thisDateStart} and {thisDateEnd} and ape<0 then chdrnum end) as this_js,
        count(distinct case when trandate between {lastDateStart} and {lastDateEnd} and ape>0 then chdrnum end)-
        count(distinct case when trandate between {lastDateStart} and {lastDateEnd} and ape<0 then chdrnum end) as last_js 
                FROM V.IPE_ACCT_BNK_KPI
                        where (trandate between {thisDateStart} and {thisDateEnd} or trandate between {lastDateStart} and {lastDateEnd})
                        {andWhere}
                                group by 
                                series,
                                branch_name,
                                agntnum)
select 
        '{category_zh}',
        '渠道',
        branch_name,
        flag,
	'',
        '{kpi_name}',
	{setMon},
        {operate_field1},
        {operate_field2},
        current timestamp 
                from mytable  
			where sys='{sys}' and (this_bf >={bf_rule} or last_bf >={bf_rule})
                        group by 
                                branch_name,
                                flag
"""
	
sql_ycrl="""
delete from administrator.hn_kpi_info_bnk_2017  where category='{category_zh}' and series='{series}' and kpi_name='{kpi_name}' and kpi_mon={setMon};
insert into administrator.hn_kpi_info_bnk_2017
with this as(
select  
        case when series in('TZ','TZF') then 'TZ' else 'SQ' end as sys,
        case when series in('FIC','TZF') then 'FIC' else f.getbranchname(branch) end branch,
        agntnum,
        agntname,
        agt_cat_desc,
        dteapp,
        dterm,
        case when dterm<{thisDateEnd} then 'current' end as curr_rl,
        case when dterm>={thisDateStart} and dteapp < {thisDateStart} then 'mon_rl' end as mon_rl       
                from administrator.hn_ipe_agntinfo_bnk a
                        where dterm >= {thisDateStart} and dteapp <= {thisDateEnd}),
last as(
select  
        case when series in('TZ','TZF') then 'TZ' else 'SQ' end as sys,
        case when series in('FIC','TZF') then 'FIC' else f.getbranchname(branch) end branch,
        agntnum,
        agntname,
        agt_cat_desc,
        dteapp,
        dterm,
        case when dterm<{lastDateEnd} then 'current' end as curr_rl,
        case when dterm>={lastDateStart}  and dteapp < {lastDateStart} then 'mon_rl' end as mon_rl       
                from administrator.hn_ipe_agntinfo_bnk a
                        where dterm >= {lastDateStart} and dteapp <= {lastDateEnd}),
sum_this as(
select 
        sys,
        branch,
        count(agntnum) as this_mon
                from this
                  where dterm >= {thisDateStart} and dteapp < {thisDateStart}
                        group by sys,branch),
sum_last as(
select 
        sys,
        branch,
        count(agntnum) as last_mon
                from last
                  where dterm >= {lastDateStart} and dteapp <= {lastDateEnd}
                        group by sys,branch)                                             
select
        '{category_zh}',
        '{series}',
        a.branch,
        '',
        '',
        '{kpi_name}',
        {setMon},
        value(sum(this_mon),0) as this_mon,
        value(sum(last_mon),0) as this_mon,
        current timestamp
                from sum_this a left join sum_last b on a.sys=b.sys and a.branch=b.branch
                        where a.sys='{sys}' 
                        group by a.branch
"""


########################################################################################################################
#########数据处理
########################################################################################################################

#设置时间
dateStart = int(time.strftime("%Y"))*10000+101
dateEnd   = int(time.strftime("%Y"))*10000+131
lastDateStart = dateStart - 10000
lastDateEnd = dateEnd - 10000
currentDate = int(time.strftime("%Y%m%d"))
lastCurrentDate = currentDate - 10000
setMon = int(time.strftime("%Y"))*100+1
last_setMon=setMon-100
currentHour=int(time.strftime("%H"))
#my_hour=16

#决定循环if
ifIsTrue = 0


#设置系列
category = [['SQ','首期'],['TZ','拓展']]

#循环：循环加工当年1-当前月的数据，数据内容由循环体决定
while(dateStart<=currentDate):
	#如果当前时间晚于9点，则跳出循环
	#if(currentHour>=my_hour and ifIsTrue==0):
	if(ifIsTrue==0):
		#当前日期-指定天数
		lastMon=datetime.datetime.now()-datetime.timedelta(days=365)
		dateStart = int(lastMon.strftime("%Y%m"))
		setMon = dateStart
		last_setMon=setMon-100
		dateStart = dateStart*100+1
		dateEnd   = dateStart+30
		lastDateStart = dateStart - 10000
		lastDateEnd = dateEnd - 10000
		ifIsTrue += 1

        #解决跨年的问题
        if(int(str(setMon)[-2:])>=13):
                dateStart = int(datetime.datetime.now().strftime("%Y%m"))*100+1
                dateEnd = int(datetime.datetime.now().strftime("%Y%m"))*100+31
                lastDateStart = dateStart - 10000
                lastDateEnd = dateEnd - 10000
                currentDate = int(time.strftime("%Y%m%d"))
                lastCurrentDate = currentDate - 10000
                setMon = int(time.strftime("%Y%m"))
                last_setMon=setMon-100

        #如果是当月，则去掉当日业绩
        if(dateEnd>currentDate):
                dateEnd=currentDate-1
                lastDateEnd = dateEnd - 10000

	
	#打印变量
	print("#########################################################################")
	print("dateStart:"+str(dateStart))
	print("dateEnd:"+str(dateEnd))
	print("setMon:"+str(setMon))
	print("ifIsTrue:"+str(ifIsTrue))



	#团队业绩循环
	for m_category in category:
		m_cate=m_category[0]
		m_cate_zh=m_category[1]

		#---------------------------------------------------------------------------------------------------
		currentSQL=sql_ycrl.format(
			category_zh=m_cate_zh,
			series='中支',
			sys=m_cate,
			kpi_name='月初人力',
			setMon=setMon,
			thisDateStart=dateStart,
			thisDateEnd=dateEnd,
			lastDateStart=lastDateStart,
			lastDateEnd=lastDateEnd,
		)
		#执行语句
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
		cur.commit()		

		#---------------------------------------------------------------------------------------------------
		currentSQL=sql_ycrl.format(
			category_zh=m_cate_zh,
			series='中支',
			sys=m_cate,
			kpi_name='当前人力',
			setMon=setMon,
			thisDateStart=dateEnd,
			thisDateEnd=dateEnd,
			lastDateStart=lastDateStart,
			lastDateEnd=lastDateEnd,
		)
		#执行语句
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
		cur.commit()		


		#---------------------------------------------------------------------------------------------------
		currentSQL=sql_yj.format(
			category_zh=m_cate_zh,
			sys=m_cate,
			kpi_name='规模保费',
			setMon=setMon,
			field='acctamt',
			thisDateStart=dateStart,
			thisDateEnd=dateEnd,
			lastDateStart=lastDateStart,
			lastDateEnd=lastDateEnd,
			andWhere="  ",
			operate_field1='sum(this_bf)',
			operate_field2='sum(last_bf)',
		)
		#执行语句
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
		cur.commit()		
		#---------------------------------------------------------------------------------------------------
		currentSQL=sql_yj.format(
			category_zh=m_cate_zh,
			sys=m_cate,
			kpi_name='期缴保费',
			setMon=setMon,
			field='acctamt',
			thisDateStart=dateStart,
			thisDateEnd=dateEnd,
			lastDateStart=lastDateStart,
			lastDateEnd=lastDateEnd,
			andWhere=" and freq='年缴'",
			operate_field1='sum(this_bf)',
			operate_field2='sum(last_bf)',
		)
		#执行语句
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
		cur.commit()		
	
		#---------------------------------------------------------------------------------------------------
		currentSQL=sql_yj.format(
			category_zh=m_cate_zh,
			sys=m_cate,
			kpi_name='趸缴保费',
			setMon=setMon,
			field='acctamt',
			thisDateStart=dateStart,
			thisDateEnd=dateEnd,
			lastDateStart=lastDateStart,
			lastDateEnd=lastDateEnd,
			andWhere=" and freq<>'年缴'",
			operate_field1='sum(this_bf)',
			operate_field2='sum(last_bf)',
		)
		#执行语句
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
		cur.commit()		
		
		#---------------------------------------------------------------------------------------------------
		currentSQL=sql_yj.format(
			category_zh=m_cate_zh,
			sys=m_cate,
			kpi_name='趸缴价值',
			setMon=setMon,
			field='acctamt_std_a',
			thisDateStart=dateStart,
			thisDateEnd=dateEnd,
			lastDateStart=lastDateStart,
			lastDateEnd=lastDateEnd,
			andWhere=" and freq<>'年缴'",
			operate_field1='sum(this_bf)',
			operate_field2='sum(last_bf)',
		)
		#执行语句
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
		cur.commit()		
		
		#---------------------------------------------------------------------------
		currentSQL=sql_yj.format(
			category_zh=m_cate_zh,
			sys=m_cate,
			kpi_name='APE保费',
			setMon=setMon,
			field='ape',
			thisDateStart=dateStart,
			thisDateEnd=dateEnd,
			lastDateStart=lastDateStart,
			lastDateEnd=lastDateEnd,
			andWhere=" and bnk_cls='高价值'",
			operate_field1='sum(this_bf)',
			operate_field2='sum(last_bf)',
		)
		#执行语句
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
		cur.commit()		

		#---------------------------------------------------------------------------
		currentSQL=sql_yj.format(
			category_zh=m_cate_zh,
			sys=m_cate,
			kpi_name='标准保费',
			setMon=setMon,
			field='acctamt_std',
			thisDateStart=dateStart,
			thisDateEnd=dateEnd,
			lastDateStart=lastDateStart,
			lastDateEnd=lastDateEnd,
			andWhere=" and bnk_cls='高价值'",
			operate_field1='sum(this_bf)',
			operate_field2='sum(last_bf)',
		)
		#执行语句
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
		cur.commit()		

		#---------------------------------------------------------------------------------------------------
		currentSQL=sql_yj.format(
			category_zh=m_cate_zh,
			sys=m_cate,
			kpi_name='APE有效保费',
			setMon=setMon,
			field='ape',
			thisDateStart=dateStart,
			thisDateEnd=dateEnd,
			lastDateStart=lastDateStart,
			lastDateEnd=lastDateEnd,
			andWhere=" and bnk_cls='高价值'",
			operate_field1='sum(case when this_bf>0 then this_bf else 0 end)',
			operate_field2='sum(case when last_bf>0 then last_bf else 0 end)',
		)
		#执行语句
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
		cur.commit()		
	
	
		#---------------------------------------------------------------------------------------------------
		currentSQL=sql_yj.format(
			category_zh=m_cate_zh,
			sys=m_cate,
			kpi_name='APE规模',
			setMon=setMon,
			field='acctamt',
			thisDateStart=dateStart,
			thisDateEnd=dateEnd,
			lastDateStart=lastDateStart,
			lastDateEnd=lastDateEnd,
			andWhere=" and bnk_cls='高价值'",
			operate_field1='sum(this_bf)',
			operate_field2='sum(last_bf)',
		)
		#执行语句
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
		cur.commit()		
	
		#---------------------------------------------------------------------------------------------------
		currentSQL=sql_yj.format(
			category_zh=m_cate_zh,
			sys=m_cate,
			kpi_name='价值保费',
			setMon=setMon,
			field='acctamt_std_a',
			thisDateStart=dateStart,
			thisDateEnd=dateEnd,
			lastDateStart=lastDateStart,
			lastDateEnd=lastDateEnd,
			andWhere=" ",
			operate_field1='sum(this_bf)',
			operate_field2='sum(last_bf)',
		)
		#执行语句
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
		cur.commit()		

		#---------------------------------------------------------------------------------------------------
		currentSQL=sql_yj.format(
			category_zh=m_cate_zh,
			sys=m_cate,
			kpi_name='承保件数',
			setMon=setMon,
			field='acctamt',
			thisDateStart=dateStart,
			thisDateEnd=dateEnd,
			lastDateStart=lastDateStart,
			lastDateEnd=lastDateEnd,
			andWhere=" ",
			operate_field1='sum(this_js)',
			operate_field2='sum(last_js)',
		)
		#执行语句
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
		cur.commit()		
	
		#---------------------------------------------------------------------------------------------------
		currentSQL=sql_yj.format(
			category_zh=m_cate_zh,
			sys=m_cate,
			kpi_name='承保件数-大个险',
			setMon=setMon,
			field='ape',
			thisDateStart=dateStart,
			thisDateEnd=dateEnd,
			lastDateStart=lastDateStart,
			lastDateEnd=lastDateEnd,
			andWhere=" and bnk_cls='高价值'",
			operate_field1='sum(this_js)',
			operate_field2='sum(last_js)',
		)
		#执行语句
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
		cur.commit()		
	
		#---------------------------------------------------------------------------------------------------
		currentSQL=sql_yj.format(
			category_zh=m_cate_zh,
			sys=m_cate,
			kpi_name='期缴有效件数',
			setMon=setMon,
			field='acctamt',
			thisDateStart=dateStart,
			thisDateEnd=dateEnd,
			lastDateStart=lastDateStart,
			lastDateEnd=lastDateEnd,
			andWhere=" and freq='年缴'",
			operate_field1='sum(case when this_js>0 then this_js else 0 end)',
			operate_field2='sum(case when last_js>0 then last_js else 0 end)',
		)
		#执行语句
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
		cur.commit()		
	
		#---------------------------------------------------------------------------------------------------
		currentSQL=sql_yj.format(
			category_zh=m_cate_zh,
			sys=m_cate,
			kpi_name='趸缴有效件数',
			setMon=setMon,
			field='acctamt',
			thisDateStart=dateStart,
			thisDateEnd=dateEnd,
			lastDateStart=lastDateStart,
			lastDateEnd=lastDateEnd,
			andWhere=" and freq<>'年缴'",
			operate_field1='sum(case when this_js>0 then this_js else 0 end)',
			operate_field2='sum(case when last_js>0 then last_js else 0 end)',
		)
		#执行语句
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
		cur.commit()		
	
		#--------------------------------------------------------------------------------
		currentSQL=sql_yj.format(
			category_zh=m_cate_zh,
			sys=m_cate,
			kpi_name='APE有效件数',
			setMon=setMon,
			field='ape',
			thisDateStart=dateStart,
			thisDateEnd=dateEnd,
			lastDateStart=lastDateStart,
			lastDateEnd=lastDateEnd,
			andWhere=" and bnk_cls='高价值'",
			operate_field1='sum(case when this_js>0 then this_js else 0 end)',
			operate_field2='sum(case when last_js>0 then last_js else 0 end)',
		)
		#执行语句
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
		cur.commit()		


		#-----------------------------------------------------------------------------
		currentSQL=sql_rl.format(
			category_zh=m_cate_zh,
			sys=m_cate,
			kpi_name='活动人力',
			setMon=setMon,
			field='ape',
			thisDateStart=dateStart,
			thisDateEnd=dateEnd,
			lastDateStart=lastDateStart,
			lastDateEnd=lastDateEnd,
			andWhere=" and bnk_cls='高价值'",
			operate_field1='count(distinct case when this_bf>=0.1 then  agntnum end)',
			operate_field2='count(distinct case when last_bf>=0.1 then  agntnum end)',
			bf_rule='0.1'
		)
		#执行语句
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
		cur.commit()		

		#-----------------------------------------------------------------------------
		currentSQL=sql_rl.format(
			category_zh=m_cate_zh,
			sys=m_cate,
			kpi_name='活动人力_标保',
			setMon=setMon,
			field='acctamt_std',
			thisDateStart=dateStart,
			thisDateEnd=dateEnd,
			lastDateStart=lastDateStart,
			lastDateEnd=lastDateEnd,
			andWhere=" and bnk_cls='高价值'",
			operate_field1='count(distinct case when this_bf>=0.1 then  agntnum end)',
			operate_field2='count(distinct case when last_bf>=0.1 then  agntnum end)',
			bf_rule='0.1'
		)
		#执行语句
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
		cur.commit()		

		#--------------------------------------------------------------------------------------
		currentSQL=sql_rl.format(
			category_zh=m_cate_zh,
			sys=m_cate,
			kpi_name='千P人力',
			setMon=setMon,
			field='ape',
			thisDateStart=dateStart,
			thisDateEnd=dateEnd,
			lastDateStart=lastDateStart,
			lastDateEnd=lastDateEnd,
			andWhere=" and bnk_cls='高价值'",
			operate_field1='count(distinct case when this_bf>=1000 then  agntnum end)',
			operate_field2='count(distinct case when last_bf>=1000 then  agntnum end)',
			bf_rule='1000'
		)
		#执行语句
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
		cur.commit()		

		#--------------------------------------------------------------------------------------
		currentSQL=sql_rl.format(
			category_zh=m_cate_zh,
			sys=m_cate,
			kpi_name='千P人力_标保',
			setMon=setMon,
			field='acctamt_std',
			thisDateStart=dateStart,
			thisDateEnd=dateEnd,
			lastDateStart=lastDateStart,
			lastDateEnd=lastDateEnd,
			andWhere=" and bnk_cls='高价值'",
			operate_field1='count(distinct case when this_bf>=1000 then  agntnum end)',
			operate_field2='count(distinct case when last_bf>=1000 then  agntnum end)',
			bf_rule='1000'
		)
		#执行语句
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
		cur.commit()		

		#-------------------------------------------------------------------------------------
		currentSQL=sql_rl.format(
			category_zh=m_cate_zh,
			sys=m_cate,
			kpi_name='3千P人力',
			setMon=setMon,
			field='ape',
			thisDateStart=dateStart,
			thisDateEnd=dateEnd,
			lastDateStart=lastDateStart,
			lastDateEnd=lastDateEnd,
			andWhere=" and bnk_cls='高价值'",
			operate_field1='count(distinct case when this_bf>=3000 then  agntnum end)',
			operate_field2='count(distinct case when last_bf>=3000 then  agntnum end)',
			bf_rule='3000'
		)
		#执行语句
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
		cur.commit()		

		#-------------------------------------------------------------------------------------
		currentSQL=sql_rl.format(
			category_zh=m_cate_zh,
			sys=m_cate,
			kpi_name='3千P人力_标保',
			setMon=setMon,
			field='acctamt_std',
			thisDateStart=dateStart,
			thisDateEnd=dateEnd,
			lastDateStart=lastDateStart,
			lastDateEnd=lastDateEnd,
			andWhere=" and bnk_cls='高价值'",
			operate_field1='count(distinct case when this_bf>=3000 then  agntnum end)',
			operate_field2='count(distinct case when last_bf>=3000 then  agntnum end)',
			bf_rule='3000'
		)
		#执行语句
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
		cur.commit()		

		#-----------------------------------------------------------------------------
		currentSQL=sql_rl.format(
			category_zh=m_cate_zh,
			sys=m_cate,
			kpi_name='5千P人力',
			setMon=setMon,
			field='ape',
			thisDateStart=dateStart,
			thisDateEnd=dateEnd,
			lastDateStart=lastDateStart,
			lastDateEnd=lastDateEnd,
			andWhere=" and bnk_cls='高价值'",
			operate_field1='count(distinct case when this_bf>=5000 then  agntnum end)',
			operate_field2='count(distinct case when last_bf>=5000 then  agntnum end)',
			bf_rule='5000'
		)
		#执行语句
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
		cur.commit()		

		#-----------------------------------------------------------------------------
		currentSQL=sql_rl.format(
			category_zh=m_cate_zh,
			sys=m_cate,
			kpi_name='5千P人力_标保',
			setMon=setMon,
			field='acctamt_std',
			thisDateStart=dateStart,
			thisDateEnd=dateEnd,
			lastDateStart=lastDateStart,
			lastDateEnd=lastDateEnd,
			andWhere=" and bnk_cls='高价值'",
			operate_field1='count(distinct case when this_bf>=5000 then  agntnum end)',
			operate_field2='count(distinct case when last_bf>=5000 then  agntnum end)',
			bf_rule='5000'
		)
		#执行语句
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
		cur.commit()		

		#-------------------------------------------------------------------------------
		currentSQL=sql_rl.format(
			category_zh=m_cate_zh,
			sys=m_cate,
			kpi_name='万P人力',
			setMon=setMon,
			field='ape',
			thisDateStart=dateStart,
			thisDateEnd=dateEnd,
			lastDateStart=lastDateStart,
			lastDateEnd=lastDateEnd,
			andWhere=" and bnk_cls='高价值'",
			operate_field1='count(distinct case when this_bf>=10000 then  agntnum end)',
			operate_field2='count(distinct case when last_bf>=10000 then  agntnum end)',
			bf_rule='10000'
		)
		#执行语句
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
		cur.commit()		

		#-------------------------------------------------------------------------------
		currentSQL=sql_rl.format(
			category_zh=m_cate_zh,
			sys=m_cate,
			kpi_name='万P人力_标保',
			setMon=setMon,
			field='acctamt_std',
			thisDateStart=dateStart,
			thisDateEnd=dateEnd,
			lastDateStart=lastDateStart,
			lastDateEnd=lastDateEnd,
			andWhere=" and bnk_cls='高价值'",
			operate_field1='count(distinct case when this_bf>=10000 then  agntnum end)',
			operate_field2='count(distinct case when last_bf>=10000 then  agntnum end)',
			bf_rule='10000'
		)
		#执行语句
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
		cur.commit()		

		#------------------------------------------------------------------------------
		currentSQL=sql_rl.format(
			category_zh=m_cate_zh,
			sys=m_cate,
			kpi_name='2万P人力',
			setMon=setMon,
			field='ape',
			thisDateStart=dateStart,
			thisDateEnd=dateEnd,
			lastDateStart=lastDateStart,
			lastDateEnd=lastDateEnd,
			andWhere=" and bnk_cls='高价值'",
			operate_field1='count(distinct case when this_bf>=20000 then  agntnum end)',
			operate_field2='count(distinct case when last_bf>=20000 then  agntnum end)',
			bf_rule='20000'
		)
		#执行语句
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
		cur.commit()		

		#------------------------------------------------------------------------------
		currentSQL=sql_rl.format(
			category_zh=m_cate_zh,
			sys=m_cate,
			kpi_name='10万P人力',
			setMon=setMon,
			field='ape',
			thisDateStart=dateStart,
			thisDateEnd=dateEnd,
			lastDateStart=lastDateStart,
			lastDateEnd=lastDateEnd,
			andWhere=" and bnk_cls='高价值'",
			operate_field1='count(distinct case when this_bf>=100000 then  agntnum end)',
			operate_field2='count(distinct case when last_bf>=100000 then  agntnum end)',
			bf_rule='100000'
		)
		#执行语句
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
		cur.commit()		


		#------------------------------------------------------------------------------
		currentSQL=sql_rl.format(
			category_zh=m_cate_zh,
			sys=m_cate,
			kpi_name='2万P人力_标保',
			setMon=setMon,
			field='acctamt_std',
			thisDateStart=dateStart,
			thisDateEnd=dateEnd,
			lastDateStart=lastDateStart,
			lastDateEnd=lastDateEnd,
			andWhere=" and bnk_cls='高价值'",
			operate_field1='count(distinct case when this_bf>=20000 then  agntnum end)',
			operate_field2='count(distinct case when last_bf>=20000 then  agntnum end)',
			bf_rule='20000'
		)
		#执行语句
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
		cur.commit()		

		#--------------------------------------------------------------------------------
		currentSQL=sql_rl.format(
			category_zh=m_cate_zh,
			sys=m_cate,
			kpi_name='3万P人力',
			setMon=setMon,
			field='ape',
			thisDateStart=dateStart,
			thisDateEnd=dateEnd,
			lastDateStart=lastDateStart,
			lastDateEnd=lastDateEnd,
			andWhere=" and bnk_cls='高价值'",
			operate_field1='count(distinct case when this_bf>=30000 then  agntnum end)',
			operate_field2='count(distinct case when last_bf>=30000 then  agntnum end)',
			bf_rule='30000'
		)
		#执行语句
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
		cur.commit()		

		#--------------------------------------------------------------------------------
		currentSQL=sql_rl.format(
			category_zh=m_cate_zh,
			sys=m_cate,
			kpi_name='3万P人力_标保',
			setMon=setMon,
			field='acctamt_std',
			thisDateStart=dateStart,
			thisDateEnd=dateEnd,
			lastDateStart=lastDateStart,
			lastDateEnd=lastDateEnd,
			andWhere=" and bnk_cls='高价值'",
			operate_field1='count(distinct case when this_bf>=30000 then  agntnum end)',
			operate_field2='count(distinct case when last_bf>=30000 then  agntnum end)',
			bf_rule='30000'
		)
		#执行语句
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
		cur.commit()		

		#-----------------------------------------------------------------------------
		currentSQL=sql_rl.format(
			category_zh=m_cate_zh,
			sys=m_cate,
			kpi_name='5万P人力',
			setMon=setMon,
			field='ape',
			thisDateStart=dateStart,
			thisDateEnd=dateEnd,
			lastDateStart=lastDateStart,
			lastDateEnd=lastDateEnd,
			andWhere=" and bnk_cls='高价值'",
			operate_field1='count(distinct case when this_bf>=50000 then  agntnum end)',
			operate_field2='count(distinct case when last_bf>=50000 then  agntnum end)',
			bf_rule='50000'
		)
		#执行语句
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
		cur.commit()		

		#-----------------------------------------------------------------------------
		currentSQL=sql_rl.format(
			category_zh=m_cate_zh,
			sys=m_cate,
			kpi_name='10万P人力',
			setMon=setMon,
			field='ape',
			thisDateStart=dateStart,
			thisDateEnd=dateEnd,
			lastDateStart=lastDateStart,
			lastDateEnd=lastDateEnd,
			andWhere=" and bnk_cls='高价值'",
			operate_field1='count(distinct case when this_bf>=100000 then  agntnum end)',
			operate_field2='count(distinct case when last_bf>=100000 then  agntnum end)',
			bf_rule='100000'
		)
		#执行语句
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
		cur.commit()		


		#-----------------------------------------------------------------------------
		currentSQL=sql_rl.format(
			category_zh=m_cate_zh,
			sys=m_cate,
			kpi_name='5万P人力_标保',
			setMon=setMon,
			field='acctamt_std',
			thisDateStart=dateStart,
			thisDateEnd=dateEnd,
			lastDateStart=lastDateStart,
			lastDateEnd=lastDateEnd,
			andWhere=" and bnk_cls='高价值'",
			operate_field1='count(distinct case when this_bf>=50000 then  agntnum end)',
			operate_field2='count(distinct case when last_bf>=50000 then  agntnum end)',
			bf_rule='50000'
		)
		#执行语句
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
		cur.commit()		


		#------------------------------------------------------------------------------------
		currentSQL=sql_yj.format(
			category_zh=m_cate_zh,
			sys=m_cate,
			kpi_name='撤单件数',
			setMon=setMon,
			field='acctamt',
			thisDateStart=dateStart,
			thisDateEnd=dateEnd,
			lastDateStart=lastDateStart,
			lastDateEnd=lastDateEnd,
			andWhere=" and statcode in('WT','撤单')",
			operate_field1='sum(this_js)',
			operate_field2='sum(last_js)',
		)
		#执行语句
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
		cur.commit()		
	
		#---------------------------------------------------------------------------------------------------
		currentSQL=sql_yj.format(
			category_zh=m_cate_zh,
			sys=m_cate,
			kpi_name='撤单保费',
			setMon=setMon,
			field='acctamt',
			thisDateStart=dateStart,
			thisDateEnd=dateEnd,
			lastDateStart=lastDateStart,
			lastDateEnd=lastDateEnd,
			andWhere=" and statcode in('WT','撤单')",
			operate_field1='sum(this_bf)',
			operate_field2='sum(last_bf)',
		)
		#执行语句
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
		cur.commit()		
	

	#------------循环体结尾，更改时间变量-------------------------------------------------------------------
	dateStart += 100
	dateEnd   += 100
	lastDateStart += 100
	lastDateEnd   += 100
	setMon += 1  
	last_setMon += 1  
	print("#############################################################################################")
	print(dateStart)
	print(setMon)
	print("#############################################################################################")

print("循环结束，开始加工当日业绩")



#--------当日业绩--------------------------------------------------------------------------------
#删除当日业绩
cur.execute("delete from administrator.hn_kpi_info_bnk_2017 where length(trim(char(kpi_mon)))=8".decode('utf-8')) 
cur.commit()

for m_category in category:
	m_cate=m_category[0]
	m_cate_zh=m_category[1]
	
	#---------------------------------------------------------------------------------------------------
	currentSQL=sql_yj.format(
		category_zh=m_cate_zh,
		sys=m_cate,
		kpi_name='APE保费',
		setMon=currentDate,
		field='ape',
		thisDateStart=currentDate,
		thisDateEnd=currentDate,
		lastDateStart=lastCurrentDate,
		lastDateEnd=lastCurrentDate,
		andWhere=" and bnk_cls='高价值'",
		operate_field1='sum(this_bf)',
		operate_field2='sum(last_bf)',
	)
	#执行语句
	for curr_sql in currentSQL.split(";"):
		cur.execute(curr_sql.decode('utf-8'))
	cur.commit()		
	

	#---------------------------------------------------------------------------------------------------
	currentSQL=sql_rl.format(
		category_zh=m_cate_zh,
		sys=m_cate,
		kpi_name='活动人力',
		setMon=currentDate,
		field='ape',
		thisDateStart=currentDate,
		thisDateEnd=currentDate,
		lastDateStart=lastCurrentDate,
		lastDateEnd=lastCurrentDate,
		andWhere=" and bnk_cls='高价值'",
		operate_field1='count(distinct agntnum)',
		operate_field2=0,
		bf_rule='0'
	)
	#执行语句
	for curr_sql in currentSQL.split(";"):
		cur.execute(curr_sql.decode('utf-8'))
	cur.commit()	

	#---------------------------------------------------------------------------------------------------
	currentSQL=sql_yj.format(
		category_zh=m_cate_zh,
		sys=m_cate,
		kpi_name='承保件数',
		setMon=currentDate,
		field='acctamt',
		thisDateStart=currentDate,
		thisDateEnd=currentDate,
		lastDateStart=lastCurrentDate,
		lastDateEnd=lastCurrentDate,
		andWhere=" ",
		operate_field1='sum(this_js)',
		operate_field2='sum(last_js)',
	)
	#执行语句
	for curr_sql in currentSQL.split(";"):
		cur.execute(curr_sql.decode('utf-8'))
	cur.commit()	

	#---------------------------------------------------------------------------------------------------
	currentSQL=sql_yj.format(
		category_zh=m_cate_zh,
		sys=m_cate,
		kpi_name='撤单件数',
		setMon=currentDate,
		field='acctamt',
		thisDateStart=currentDate,
		thisDateEnd=currentDate,
		lastDateStart=lastCurrentDate,
		lastDateEnd=lastCurrentDate,
		andWhere=" and statcode in('WT','CD','CF')",
		operate_field1='sum(this_js)',
		operate_field2='sum(last_js)',
	)
	#执行语句
	for curr_sql in currentSQL.split(";"):
		cur.execute(curr_sql.decode('utf-8'))
	cur.commit()		
	
	#---------------------------------------------------------------------------------------------------
	currentSQL=sql_yj.format(
		category_zh=m_cate_zh,
		sys=m_cate,
		kpi_name='撤单保费',
		setMon=currentDate,
		field='acctamt',
		thisDateStart=currentDate,
		thisDateEnd=currentDate,
		lastDateStart=lastCurrentDate,
		lastDateEnd=lastCurrentDate,
		andWhere=" and statcode in('WT','CD','CF')",
		operate_field1='sum(this_bf)',
		operate_field2='sum(last_bf)',
	)
	#执行语句
	for curr_sql in currentSQL.split(";"):
		cur.execute(curr_sql.decode('utf-8'))
	cur.commit()		
	
#-----------------------------------------------------------------------------------------------------
print("加工中支业绩")
cur.execute("delete from administrator.hn_kpi_info_bnk_2017 where series='中支' and kpi_name not in('月初人力','当前人力')".decode('utf-8'))
curr_sql="""
insert into administrator.hn_kpi_info_bnk_2017 
select 
        category,
        '中支',
        branch,
        '',
        '',
        kpi_name,
        kpi_mon,
        sum(this_mon),
        sum(last_mon),
        current timestamp                       
                from administrator.hn_kpi_info_bnk_2017
                        where series='渠道'
                                group by 
                                        category,
                                        branch,
                                        kpi_name,
                                        kpi_mon
"""
cur.execute(curr_sql.decode('utf-8')) 

print("加工县域业绩")
cur.execute("delete from administrator.hn_kpi_info_bnk_2017 where series='县域'".decode('utf-8'))
curr_sql="""
insert into administrator.hn_kpi_info_bnk_2017 
select 
        category,
        '县域',
        branch,
        aracde,
        '',
        kpi_name,
        kpi_mon,
        sum(this_mon),
        sum(last_mon),
        current timestamp                       
                from administrator.hn_kpi_info_bnk_2017
                        where series='渠道'
                                group by 
                                        category,
                                        branch,
					aracde,
                                        kpi_name,
                                        kpi_mon
"""
cur.execute(curr_sql.decode('utf-8')) 


#--------------------------------------------------------------------------------------------------------
print("加工银保业绩")
cur.execute("delete from administrator.hn_kpi_info_bnk_2017 where category='银保'".decode('utf-8'))
curr_sql="""
insert into administrator.hn_kpi_info_bnk_2017 
select 
        '银保',
        series,
        branch,
        aracde,
        medi_code,
        kpi_name,
        kpi_mon,
        sum(this_mon),
        sum(last_mon),
        current timestamp                       
                from administrator.hn_kpi_info_bnk_2017
                                group by 
                                        series,
                                        branch,
                                        aracde,
                                        medi_code,
                                        kpi_name,
                                        kpi_mon
"""
cur.execute(curr_sql.decode('utf-8')) 
cur.execute('update administrator.hn_kpi_info_bnk_2017 set branch=trim(branch)'.decode('utf-8')) 

cur.commit()			
cur.close()
