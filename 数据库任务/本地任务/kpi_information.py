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
conn=pyodbc.connect('DRIVER={DB2};SERVER=10.19.19.34;DATABASE=HNII;UID=report;PWD=okm123;charset=utf-8')
cur = conn.cursor()

#设置SQL语句
sql_yj="""
delete from administrator.hn_kpi_information where category='{category_zh}' and series='{series}' and kpi_name='{kpi_name}' and kpi_mon={setMon};
insert into administrator.hn_kpi_information
with
team_kpi_this as(
		select
		branch,
		aracde,
		partnum,
		teamnum,
		{selectFieled} as sum_this
		from table(f.get_gx_team_kpi({thisDateStart},{thisDateEnd},'{category}',{rangeStart},{rangeEnd}))a ),
	      team_kpi_last as(
			      select
			      branch,
			      aracde,
			      partnum,
			      teamnum,
			      {selectFieled} as sum_last
			      from table(f.get_gx_team_kpi({lastDateStart},{lastDateEnd},'{category}',{rangeStart},{rangeEnd}))a )    
	    select 
	      '{category_zh}' as categroy,
	      trim('{series}') as series,
	      trim(f.getbranchname(case when a.branch is null then b.branch else a.branch end)) as branch,
	      trim(administrator.getaracdename(case when a.aracde is null then b.aracde else a.aracde end)) as aracde,
	      value((select trim(agntname)||'部' from administrator.hn_agntinfo c where c.agntnum=a.partnum),'无头部') as partname,
	      value((select trim(agntname)||'组' from administrator.hn_agntinfo c where c.agntnum=a.teamnum),'无头组') as teamname,
	      '{kpi_name}'as kpi_name,
{setMon} as mon,
	value(sum_this,0) as this_mon,
	value(sum_last,0) as last_mon,
	current timestamp as currentTime
	from team_kpi_this a full join team_kpi_last b 
	on a.branch=b.branch and a.aracde=b.aracde
	and a.partnum=b.partnum and a.teamnum=b.teamnum
"""

sql_rl="""
delete from administrator.hn_kpi_information where category='{category_zh}' and series = '{series}' and kpi_name ='{kpi_name}' and kpi_mon={setMon};
insert into administrator.hn_kpi_information
	with 
	team_rl_this as(
			select 
			branch,
			aracde,
			partnum,
			teamnum,
			{selectFieled} as this_rl
			from table(f.get_gx_teamname({thisDateStart},{thisDateEnd},'{category}'))a),
	team_rl_last as(
			select 
			branch,
			aracde,
			partnum,
			teamnum,
			{selectFieled} as last_rl
			from table(f.get_gx_teamname({lastDateStart},{lastDateEnd},'{category}'))a)
	select 
	'{category_zh}' as categroy,
	'{series}' as series,
	trim(f.getbranchname(case when a.branch is null then b.branch else a.branch end)) as branch,
	trim(administrator.getaracdename(case when a.aracde is null then b.aracde else a.aracde end)) as aracde,
	case when a.partnum is null 
	then (select trim(agntname)||'部' from administrator.hn_agntinfo c where c.agntnum=b.partnum)
	else (select trim(agntname)||'部' from administrator.hn_agntinfo c where c.agntnum=a.partnum) end as partname,
	case when a.teamnum is null 
	then (select trim(agntname)||'组' from administrator.hn_agntinfo c where c.agntnum=b.teamnum)
	else (select trim(agntname)||'组' from administrator.hn_agntinfo c where c.agntnum=a.teamnum) end as teamname,
	'{kpi_name}'as kpi_name,
{setMon} as mon,
	value(this_rl,0) as this_mon,
	value(last_rl,0) as last_mon,
	current timestamp as currentTime
	from team_rl_this a full join team_rl_last b 
	on a.branch=b.branch and a.aracde=b.aracde
	and a.partnum=b.partnum and a.teamnum=b.teamnum
"""

sql_sum="""
delete from administrator.hn_kpi_information where branch='合计' and kpi_name ='{kpi_name}' ;
insert into administrator.hn_kpi_information
	select 
	category,
	series,
	'合计',
	'',
	'',
	'',
	kpi_name,
	kpi_mon,
	sum(this_mon),
	sum(last_mon),
	processing_time
	from  administrator.hn_kpi_information
	where kpi_name='{kpi_name}'
	group by         
	category,
	series,
	kpi_name,
	kpi_mon,
	processing_time
"""

#主管人力
sql_zg="""
delete from administrator.hn_kpi_information where category='{category_zh}' and series='{series}' and kpi_name='{kpi_name}' and kpi_mon={setMon};
insert into administrator.hn_kpi_information
select 
	'{category_zh}' as categroy,
      	trim('{series}') as series,
      	trim(f.getbranchname(branch)) as branch,
      	trim(administrator.getaracdename(aracde)) as aracde,
      	partname,
	teamname,
	'{kpi_name}',
	int(decimal(current date)/100) as mon,
	count(agntnum),
	0,
	current timestamp as currentTime
        	from administrator.hn_agntinfo
        		where dtetrm=99999999
        		and agtype in('AS','SS','UM','AD') 
                		group by 
                        	branch,
                       	 	aracde,
	                        partname,
       		                teamname
"""
	
sql_newrl_kpi="""
delete from administrator.hn_kpi_information where category='{category_zh}' and series='{series}' and kpi_name='{kpi_name}' and kpi_mon={setMon};
insert into administrator.hn_kpi_information
with chdr as(
select 
        f.getbranchname(a.branch) as branch,
        administrator.getaracdename(a.aracde) as aracde,
        case when trim(a.partname)='' or a.partname is null or trim(a.partname)='部' then '无头部' else value(trim(a.partname),'无头部') end as partname,
        case when trim(a.teamname)='' or a.teamname is null or trim(a.teamname)='组' then '无头组' else value(trim(a.teamname),'无头组') end as teamname,
        a.agntnum,
        case when a.agtype='RC' then 'SZ' else 'YX' end as agtype,
        a.chdrnum,
        sum(acctamt_std) as bf,
        case when sum(acctamt_std)>0 then 1 when sum(acctamt_std)<0 then -1 end as js
        from administrator.hn_acctinfo a,administrator.hn_agntinfo b
                where a.trandate between {thisDateStart} and {thisDateEnd}
                and a.agntnum=b.agntnum
                and b.dteapp between {thisDateStart} and {thisDateEnd}
                and a.agtype IN ('TA','SA','TS','SM','SD','SE','AS','SS','UM','AD','HD','RC')
                and batctrcde<>'TGJC'
                        group by 
                                a.branch,
                                a.aracde,
                                a.partname,
                                a.teamname,
                                a.agntnum,
                                a.agtype,
                                a.chdrnum),
rl as(                                
select 
        branch,
        aracde,
        partname,
        teamname,
        agntnum,
        agtype,
        sum(bf) as bf,
        sum(js) as js
                from chdr
                        group by 
                                branch,
                                aracde,
                                partname,
                                teamname,
                                agntnum,
                                agtype)                                                               
select 
        '{category_zh}',
        '{series}',
        branch,
        aracde,
        partname,
        teamname,
        '{kpi_name}',
        {setMon},
        {operate_field},
        0 as last_mon,
        current timestamp as currentTime
        from rl
                where agtype='{category}'
                        group by 
                                branch,
                                aracde,
                                partname,
                                teamname
                               	{havingby} 
"""
#脱落人力20150716_
tlrl="""
delete from administrator.hn_kpi_information where category='{category_zh}' and series='{series}' and kpi_name='{kpi_name}' and kpi_mon={setMon};
insert into administrator.hn_kpi_information
with tlrl as(
select 
        f.getbranchname(branch) as branch,
        administrator.getaracdename(aracde) as aracde,
        case when trim(partname)='' or partname is null or trim(partname)='部' then '无头部' else trim(partname) end as partname,
        case when trim(teamname)='' or teamname is null or trim(teamname)='组' then '无头组' else trim(teamname) end as teamname,
        case when dtetrm between {thisDateStart} and {thisDateEnd} then agntnum end as this_agnt,
        case when dtetrm between {lastDateStart} and {lastDateEnd} then agntnum end as last_agnt,
        case when agtype='RC' then 'SZ' else 'YX' end as agtype
        from administrator.hn_agntinfo
                where (dtetrm between {thisDateStart} and {thisDateEnd}
                or dtetrm between {lastDateStart} and {lastDateEnd})
                and agtype in ('TA','SA','TS','SM','SD','SE','AS','SS','UM','AD','HD','RC'))
select 
        '{category_zh}',
        '{series}',
        branch,
        aracde,
        partname,
        teamname,
        '{kpi_name}',
        {setMon},
        count(this_agnt),
        count(last_agnt),
        current timestamp as currentTime
        from tlrl
                where agtype='{category}'
                        group by 
                                branch,
                                aracde,
                                partname,
                                teamname

"""
#优化后的人力语句20150618
sql_yj_new="""
delete from administrator.hn_kpi_information where category='{category_zh}' and series='{series}' and kpi_name='{kpi_name}' and kpi_mon={setMon};
insert into administrator.hn_kpi_information
with of_chdrinfo as(
select 
        branch,
        aracde,
        case when partnum ='' or partnum is null then '99999999' else partnum end as partnum,
        case when teamnum ='' or teamnum is null then '99999999' else teamnum end as teamnum,
        agntnum,
        case when agtype='RC' then 'SZ' when agtype in('TR','TB') then 'DX' when agtype in('HA') then 'JD' else 'YX' end as sys,
        --chdrnum,
        sum(case when trandate between {thisDateStart} and {thisDateEnd} then acctamt_std end) as this_bf,
        sum(case when trandate between {lastDateStart} and {lastDateEnd} then acctamt_std end) as last_bf,
        sum(case when trandate=decimal(current date) then acctamt_std end) as this_curr_yj,
        sum(case when trandate=decimal(current date -1 year) then acctamt_std end) as last_curr_yj,
        case when sum(case when trandate = decimal(current date) then acctamt_std end)=
                sum(case when trandate between {thisDateStart} and decimal(current date) then acctamt_std end) 
                then agntnum end as this_is_new_rl,
        case when sum(case when trandate = decimal(current date - 1 year) then acctamt_std end)=
                sum(case when trandate between {lastDateStart} and decimal(current date -1 year) then acctamt_std end) 
                then agntnum end as last_is_new_rl             
                from administrator.hn_acctinfo
                        where (trandate between {thisDateStart} and {thisDateEnd} 
                        or trandate between {lastDateStart} and {lastDateEnd} 
                        or trandate=decimal(current date)
                        or trandate=decimal(current date - 1 year))
                        and batctrcde<>'TGJC'
                        and agtype in('TA','SA','SM','SD','AS','SS','UM','AD','SE','HD','TS','RC','TR','TB','HA')
                                group by 
                                branch,
                                aracde,
                                partnum,
                                teamnum,
                                agntnum,
                                --chdrnum,
                                agtype)
select 
        '{category_zh}' as categroy,
        trim('营业组') as series,
        trim(f.getbranchname(a.branch)) as branch,
        trim(administrator.getaracdename(a.aracde)) as aracde,
        (select trim(agntname)||'部' from administrator.hn_agntinfo c where c.agntnum=a.partnum) as partname,
        (select trim(agntname)||'组' from administrator.hn_agntinfo c where c.agntnum=a.teamnum) as teamname,
        '{kpi_name}'as kpi_name,
        {setMon} as mon,
        count(distinct case when {thisField}>={rule} then {thisCount} end) as this_mon,
        count(distinct case when {lastField}>={rule} then {lastCount} end) as this_mon,
        current timestamp as timest
                from of_chdrinfo a
                        where sys='{category}'
                                group by 
                                branch,
                                aracde,
                                partnum,
                                teamnum

"""

sl_rl="""
delete from administrator.hn_kpi_information where category='{category_zh}' and series='{series}' and kpi_name='{kpi_name}' and kpi_mon={setMon};
insert into administrator.hn_kpi_information
with of_chdrinfo as(
select 
        branch,
        aracde,
        case when partnum ='' or partnum is null then '99999999' else partnum end as partnum,
        case when teamnum ='' or teamnum is null then '99999999' else teamnum end as teamnum,
        agntnum,
        case when agtype='RC' then 'SZ' when agtype in('TR','TB') then 'DX' when agtype in('HA') then 'JD' else 'YX' end as sys,
        sum(case when HPRRCVDT between {thisDateStart} and {thisDateEnd} then acctamt_std end) as this_bf,
        sum(case when HPRRCVDT={thisDateEnd} then acctamt_std end) as this_curr_yj
                from administrator.hn_ipe_hpad
                        where (hprrcvdt between {thisDateStart} and {thisDateEnd} 
                        or hprrcvdt=decimal(current date))
                        and agtype in('TA','SA','SM','SD','AS','SS','UM','AD','SE','HD','TS','RC','TR','TB','HA')
                                group by 
                                branch,
                                aracde,
                                partnum,
                                teamnum,
                                agntnum,
                                agtype)
select 
        '{category_zh}' as categroy,
        trim('营业组') as series,
        trim(f.getbranchname(a.branch)) as branch,
        trim(administrator.getaracdename(a.aracde)) as aracde,
        (select trim(agntname)||'部' from administrator.hn_agntinfo c where c.agntnum=a.partnum) as partname,
        (select trim(agntname)||'组' from administrator.hn_agntinfo c where c.agntnum=a.teamnum) as teamname,
        '{kpi_name}'as kpi_name,
        {setMon} as mon,
        count(distinct case when {thisField}>={rule} then {thisCount} end) as this_mon,
        0,
        current timestamp as timest
                from of_chdrinfo a
                        where sys='{category}'
                                group by 
                                branch,
                                aracde,
                                partnum,
                                teamnum

"""


#优化后的当日人力语句20160324
sql_rl_current="""
delete from administrator.hn_kpi_information where category='{category_zh}' and series='{series}' and kpi_name='{kpi_name}' and kpi_mon={setMon};
insert into administrator.hn_kpi_information
with of_chdrinfo as(
select
        branch,
        aracde,
        case when partnum ='' or partnum is null then '99999999' else partnum end as partnum,
        case when teamnum ='' or teamnum is null then '99999999' else teamnum end as teamnum,
        agntnum,
        case when agtype='RC' then 'SZ' when agtype in('TR','TB') then 'DX' when agtype in('HA') then 'JD' else 'YX' end as sys,
        sum(case when trandate between int(decimal(current date)/100)*100+1
            and decimal(current date) then acctamt_std end) as this_bf,
        sum(case when trandate between int(decimal(current date)/100)*100+1
            and decimal(current date - 1 days) then acctamt_std end) as per_bf
                from administrator.hn_acctinfo
                        where trandate between decimal(current date - day(current date -1 days) days) 
                        and decimal(current date)
                        and batctrcde<>'TGJC'
                        and agtype in('TA','SA','SM','SD','AS','SS','UM','AD','SE','HD','TS','RC','TR','TB','HA')
                                group by
                                branch,
                                aracde,
                                partnum,
                                teamnum,
                                agntnum,
                                agtype)
select 
        '{category_zh}' as categroy,
        trim('营业组') as series,
        trim(f.getbranchname(a.branch)) as branch,
        trim(administrator.getaracdename(a.aracde)) as aracde,
        (select trim(agntname)||'部' from administrator.hn_agntinfo c where c.agntnum=a.partnum) as partname,
        (select trim(agntname)||'组' from administrator.hn_agntinfo c where c.agntnum=a.teamnum) as teamname,
        '{kpi_name}'as kpi_name,
        {setMon} as mon,
        count( case when value(this_bf,0)>={rule} and value(per_bf,0)<{rule} then agntnum end) as this_mon,
	0,
        current timestamp as timest
                from of_chdrinfo a
                        where sys='{category}'
                                group by 
                                branch,
                                aracde,
                                partnum,
                                teamnum

"""

#优化后的当日人力语句20160324
sl_rl_curr="""
delete from administrator.hn_kpi_information where category='{category_zh}' and series='{series}' and kpi_name='{kpi_name}' and kpi_mon={setMon};
insert into administrator.hn_kpi_information
with of_chdrinfo as(
select
        branch,
        aracde,
        case when partnum ='' or partnum is null then '99999999' else partnum end as partnum,
        case when teamnum ='' or teamnum is null then '99999999' else teamnum end as teamnum,
        agntnum,
        case when agtype='RC' then 'SZ' when agtype in('TR','TB') then 'DX' when agtype in('HA') then 'JD' else 'YX' end as sys,
        sum(case when hprrcvdt between int(decimal(current date)/100)*100+1
            and decimal(current date) then acctamt_std end) as this_bf,
        sum(case when hprrcvdt between int(decimal(current date)/100)*100+1
            and decimal(current date - 1 days) then acctamt_std end) as per_bf
                from administrator.hn_ipe_hpad
                        where hprrcvdt between decimal(current date - day(current date -1 days) days) 
                        and decimal(current date)
                        and agtype in('TA','SA','SM','SD','AS','SS','UM','AD','SE','HD','TS','RC','TR','TB','HA')
                                group by
                                branch,
                                aracde,
                                partnum,
                                teamnum,
                                agntnum,
                                agtype)
select 
        '{category_zh}' as categroy,
        trim('营业组') as series,
        trim(f.getbranchname(a.branch)) as branch,
        trim(administrator.getaracdename(a.aracde)) as aracde,
        (select trim(agntname)||'部' from administrator.hn_agntinfo c where c.agntnum=a.partnum) as partname,
        (select trim(agntname)||'组' from administrator.hn_agntinfo c where c.agntnum=a.teamnum) as teamname,
        '{kpi_name}'as kpi_name,
        {setMon} as mon,
        count( case when value(this_bf,0)>={rule} and value(per_bf,0)<{rule} then agntnum end) as this_mon,
	0,
        current timestamp as timest
                from of_chdrinfo a
                        where sys='{category}'
                                group by 
                                branch,
                                aracde,
                                partnum,
                                teamnum

"""

#优化后的人力语句20150618
sql_yj_new_zg="""
delete from administrator.hn_kpi_information where category='{category_zh}' and series='{series}' and kpi_name='{kpi_name}' and kpi_mon={setMon};
insert into administrator.hn_kpi_information
with of_chdrinfo as(
select 
        branch,
        aracde,
        case when partnum ='' or partnum is null then '99999999' else partnum end as partnum,
        case when teamnum ='' or teamnum is null then '99999999' else teamnum end as teamnum,
        agntnum,
        case when agtype='RC' then 'SZ' when agtype in('TR','TB') then 'DX' when agtype in('HA') then 'JD' else 'YX' end as sys,
        --chdrnum,
        sum(case when trandate between {thisDateStart} and {thisDateEnd} then acctamt_std end) as this_bf,
        sum(case when trandate between {lastDateStart} and {thisDateEnd} then acctamt_std end) as last_bf,
        sum(case when trandate=decimal(current date) then acctamt_std end) as this_curr_yj,
        sum(case when trandate=decimal(current date -1 year) then acctamt_std end) as last_curr_yj,
        case when sum(case when trandate = decimal(current date) then acctamt_std end)=
                sum(case when trandate between {thisDateStart} and decimal(current date) then acctamt_std end) 
                then agntnum end as this_is_new_rl,
        case when sum(case when trandate = decimal(current date - 1 year) then acctamt_std end)=
                sum(case when trandate between {lastDateStart} and decimal(current date -1 year) then acctamt_std end) 
                then agntnum end as last_is_new_rl             
                from administrator.hn_acctinfo
                        where (trandate between {thisDateStart} and {thisDateEnd} 
                        or trandate between {lastDateStart} and {lastDateEnd} 
                        or trandate=decimal(current date)
                        or trandate=decimal(current date - 1 year))
                        and batctrcde<>'TGJC'
                        and agtype in('AS','SS','UM','AD')
                                group by 
                                branch,
                                aracde,
                                partnum,
                                teamnum,
                                agntnum,
                                --chdrnum,
                                agtype)
select 
        '{category_zh}' as categroy,
        trim('营业组') as series,
        trim(f.getbranchname(a.branch)) as branch,
        trim(administrator.getaracdename(a.aracde)) as aracde,
        (select trim(agntname)||'部' from administrator.hn_agntinfo c where c.agntnum=a.partnum) as partname,
        (select trim(agntname)||'组' from administrator.hn_agntinfo c where c.agntnum=a.teamnum) as teamname,
        '{kpi_name}'as kpi_name,
        {setMon} as mon,
        count( case when {thisField}>={rule} then {thisCount} end) as this_mon,
        count( case when {lastField}>={rule} then {lastCount} end) as this_mon,
        current timestamp as timest
                from of_chdrinfo a
                        where sys='{category}'
                                group by 
                                branch,
                                aracde,
                                partnum,
                                teamnum

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
my_hour=6

#决定循环if
ifIsTrue = 0


#设置系列
category = [['YX','营销'],['SZ','收展'],['DX','电销'],['JD','经代']]

#循环：循环加工当年1-当前月的数据，数据内容由循环体决定
while(dateStart<=currentDate):
	#如果当前时间晚于9点，则跳出循环
	if(currentHour>=my_hour and ifIsTrue==0):
		#当前日期-指定天数
		lastMon=datetime.datetime.now()-datetime.timedelta(days=2)
		dateStart = int(lastMon.strftime("%Y%m"))
		setMon = dateStart
		last_setMon=setMon-100
		dateStart = dateStart*100+1
		dateEnd   = dateStart+30
		lastDateStart = dateStart - 10000
		lastDateEnd = dateEnd - 10000
                ifIsTrue += 1
	#决定是否加工当月
	if(currentHour>10):
		print("跳出当月业绩加工，直接加工当日业绩")
	     	break

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
        if(dateEnd>=currentDate):
                dateEnd=currentDate-1
		lastDateEnd = dateEnd - 10000


	#主管人力
	currentSQL=sql_zg.format(
		category_zh='营销',
		series='营业组',
		kpi_name='主管人力',
		setMon=setMon,
		last_setMon=last_setMon
	)
	#执行语句
	for curr_sql in currentSQL.split(";"):
		cur.execute(curr_sql.decode('utf-8'))
	cur.commit()			

	#营销，收展循环
	for m_category in category:
		m_cate=m_category[0]
		m_cate_zh=m_category[1]
		print(m_cate+":"+str(dateStart)+" "+str(dateEnd))		

		#-------------------------------------------------------------------------------
		currentSQL=tlrl.format(
			category=m_cate,
			category_zh=m_cate_zh,
			series='营业组',
			kpi_name='脱落人力',
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


		#-------------------------------------------------------------------------------
		currentSQL=sql_newrl_kpi.format(
			category=m_cate,
			category_zh=m_cate_zh,
			series='营业组',
			kpi_name='新人承保件数',
			setMon=setMon,
			thisDateStart=dateStart,
			thisDateEnd=dateEnd,
			operate_field="value(sum(js),0)",
			havingby='',
		)
		#执行语句
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
		cur.commit()			

		#-------------------------------------------------------------------------------
		currentSQL=sql_newrl_kpi.format(
			category=m_cate,
			category_zh=m_cate_zh,
			series='营业组',
			kpi_name='新人标准保费',
			setMon=setMon,
			thisDateStart=dateStart,
			thisDateEnd=dateEnd,
			operate_field="sum(bf)",
			havingby='',
		)
		#执行语句
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
		cur.commit()			

		#-------------------------------------------------------------------------------
		currentSQL=sql_newrl_kpi.format(
			category=m_cate,
			category_zh=m_cate_zh,
			series='营业组',
			kpi_name='新人活动人力',
			setMon=setMon,
			thisDateStart=dateStart,
			thisDateEnd=dateEnd,
			operate_field="count(distinct agntnum)",
			havingby='having sum(bf)>0',
		)
		#执行语句
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
		cur.commit()			

		#-------------------------------------------------------------------------------
		currentSQL=sql_newrl_kpi.format(
			category=m_cate,
			category_zh=m_cate_zh,
			series='营业组',
			kpi_name='新人3千P人力',
			setMon=setMon,
			thisDateStart=dateStart,
			thisDateEnd=dateEnd,
			operate_field="count(distinct agntnum)",
			havingby='having sum(bf)>=3000',
		)
		#执行语句
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
		cur.commit()			

		#-------------------------------------------------------------------------------
		currentSQL=sql_yj.format(
			category=m_cate,
			category_zh=m_cate_zh,
			series='营业组',
			kpi_name='规模保费',
			selectFieled='gmbf_range',
			setMon=setMon,
			thisDateStart=dateStart,
			thisDateEnd=dateEnd,
			lastDateStart=lastDateStart,
			lastDateEnd=lastDateEnd,
			rangeStart=-50000000,
			rangeEnd=50000000,
		)
		#执行语句
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
		cur.commit()			

		#--------营销标准保费--------------------------------------------------------------------------------
		#设定SQL语句参数
		currentSQL=sql_yj.format(
			category=m_cate,
			category_zh=m_cate_zh,
			series='营业组',
			kpi_name='标准保费',
			selectFieled='bzbf_range',
			setMon=setMon,
			thisDateStart=dateStart,
			thisDateEnd=dateEnd,
			lastDateStart=lastDateStart,
			lastDateEnd=lastDateEnd,
			rangeStart=-50000000,
			rangeEnd=50000000,
		)
		#执行语句
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
		cur.commit()			

		#-----------------------------------------------------------------------------------
		#设定SQL语句参数
		currentSQL=sql_yj.format(
			category=m_cate,
			category_zh=m_cate_zh,
			series='营业组',
			kpi_name='价值保费',
			selectFieled='jzbf_range',
			setMon=setMon,
			thisDateStart=dateStart,
			thisDateEnd=dateEnd,
			lastDateStart=lastDateStart,
			lastDateEnd=lastDateEnd,
			rangeStart=-50000000,
			rangeEnd=50000000,
		)
		#执行语句
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
		cur.commit()			

		#-------------------------------------------------------------------------------------
		#设定SQL语句参数
		currentSQL=sql_yj.format(
			category=m_cate,
			category_zh=m_cate_zh,
			series='营业组',
			kpi_name='承保件数',
			selectFieled='js_range',
			setMon=setMon,
			thisDateStart=dateStart,
			thisDateEnd=dateEnd,
			lastDateStart=lastDateStart,
			lastDateEnd=lastDateEnd,
			rangeStart=-50000000,
			rangeEnd=50000000,
		)
		#执行语句
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
		cur.commit()			

		#-------------------------------------------------------------------------------------
		#设定SQL语句参数
		currentSQL=sql_yj.format(
			category=m_cate,
			category_zh=m_cate_zh,
			series='营业组',
			kpi_name='活动人力',
			selectFieled='rl_range',
			setMon=setMon,
			thisDateStart=dateStart,
			thisDateEnd=dateEnd,
			lastDateStart=lastDateStart,
			lastDateEnd=lastDateEnd,
			rangeStart=0,
			rangeEnd=50000000,
		)
		#执行语句
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
		cur.commit()			

		#--------营销千P人力--------------------------------------------------------------------------------
		#设定SQL语句参数
		currentSQL=sql_yj_new.format(
			category=m_cate,
			category_zh=m_cate_zh,
			series='营业组',
			kpi_name='千P人力',
			setMon=setMon,
			thisDateStart=dateStart,
			thisDateEnd=dateEnd,
			lastDateStart=lastDateStart,
			lastDateEnd=lastDateEnd,
			rule=1000,
			thisField='this_bf',
			lastField='last_bf',
			thisCount='agntnum',
			lastCount='agntnum',
		)
		#执行语句
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
		cur.commit()			


		#--------营销3千P人力--------------------------------------------------------------------------------
		#设定SQL语句参数
		currentSQL=sql_yj_new.format(
			category=m_cate,
			category_zh=m_cate_zh,
			series='营业组',
			kpi_name='3千P人力',
			setMon=setMon,
			thisDateStart=dateStart,
			thisDateEnd=dateEnd,
			lastDateStart=lastDateStart,
			lastDateEnd=lastDateEnd,
			rule=3000,
			thisField='this_bf',
			lastField='last_bf',
			thisCount='agntnum',
			lastCount='agntnum',
		)
		#执行语句
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
		cur.commit()			

		#--------受理3千P人力--------------------------------------------------------------------------------
		#设定SQL语句参数
		currentSQL=sl_rl.format(
			category=m_cate,
			category_zh=m_cate_zh,
			series='营业组',
			kpi_name='受理3千P人力',
			setMon=setMon,
			thisDateStart=dateStart,
			thisDateEnd=dateEnd,
			rule=3000,
			thisField='this_bf',
			lastField='last_bf',
			thisCount='agntnum',
			lastCount='agntnum',
		)
		#执行语句
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
		cur.commit()			

		#--------受理1万P人力--------------------------------------------------------------------------------
		#设定SQL语句参数
		currentSQL=sl_rl.format(
			category=m_cate,
			category_zh=m_cate_zh,
			series='营业组',
			kpi_name='受理1万P人力',
			setMon=setMon,
			thisDateStart=dateStart,
			thisDateEnd=dateEnd,
			rule=10000,
			thisField='this_bf',
			lastField='last_bf',
			thisCount='agntnum',
			lastCount='agntnum',
		)
		#执行语句
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
		cur.commit()			

		#--------受理3万P人力--------------------------------------------------------------------------------
		#设定SQL语句参数
		currentSQL=sl_rl.format(
			category=m_cate,
			category_zh=m_cate_zh,
			series='营业组',
			kpi_name='受理3万P人力',
			setMon=setMon,
			thisDateStart=dateStart,
			thisDateEnd=dateEnd,
			rule=30000,
			thisField='this_bf',
			lastField='last_bf',
			thisCount='agntnum',
			lastCount='agntnum',
		)
		#执行语句
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
		cur.commit()			


		#--------受理5万P人力--------------------------------------------------------------------------------
		#设定SQL语句参数
		currentSQL=sl_rl.format(
			category=m_cate,
			category_zh=m_cate_zh,
			series='营业组',
			kpi_name='受理5万P人力',
			setMon=setMon,
			thisDateStart=dateStart,
			thisDateEnd=dateEnd,
			rule=50000,
			thisField='this_bf',
			lastField='last_bf',
			thisCount='agntnum',
			lastCount='agntnum',
		)
		#执行语句
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
		cur.commit()			






		#--------营销5千P人力--------------------------------------------------------------------------------
		#设定SQL语句参数
		currentSQL=sql_yj_new.format(
			category=m_cate,
			category_zh=m_cate_zh,
			series='营业组',
			kpi_name='5千P人力',
			setMon=setMon,
			thisDateStart=dateStart,
			thisDateEnd=dateEnd,
			lastDateStart=lastDateStart,
			lastDateEnd=lastDateEnd,
			rule=5000,
			thisField='this_bf',
			lastField='last_bf',
			thisCount='agntnum',
			lastCount='agntnum',
		)
		#执行语句
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
		cur.commit()			


		#--------营销6千P人力--------------------------------------------------------------------------------
		#设定SQL语句参数
		currentSQL=sql_yj_new.format(
			category=m_cate,
			category_zh=m_cate_zh,
			series='营业组',
			kpi_name='6千P人力',
			setMon=setMon,
			thisDateStart=dateStart,
			thisDateEnd=dateEnd,
			lastDateStart=lastDateStart,
			lastDateEnd=lastDateEnd,
			rule=6000,
			thisField='this_bf',
			lastField='last_bf',
			thisCount='agntnum',
			lastCount='agntnum',
		)
		#执行语句
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
		cur.commit()			



		#--------营销万P人力--------------------------------------------------------------------------------
		#设定SQL语句参数
		currentSQL=sql_yj_new.format(
			category=m_cate,
			category_zh=m_cate_zh,
			series='营业组',
			kpi_name='1万P人力',
			setMon=setMon,
			thisDateStart=dateStart,
			thisDateEnd=dateEnd,
			lastDateStart=lastDateStart,
			lastDateEnd=lastDateEnd,
			rule=10000,
			thisField='this_bf',
			lastField='last_bf',
			thisCount='agntnum',
			lastCount='agntnum',
		)
		#执行语句
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
		cur.commit()			

		#--------营销2万P人力--------------------------------------------------------------------------------
		#设定SQL语句参数
		currentSQL=sql_yj_new.format(
			category=m_cate,
			category_zh=m_cate_zh,
			series='营业组',
			kpi_name='2万P人力',
			setMon=setMon,
			thisDateStart=dateStart,
			thisDateEnd=dateEnd,
			lastDateStart=lastDateStart,
			lastDateEnd=lastDateEnd,
			rule=20000,
			thisField='this_bf',
			lastField='last_bf',
			thisCount='agntnum',
			lastCount='agntnum',
		)
		#执行语句
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
		cur.commit()			

		#--------营销3万P人力--------------------------------------------------------------------------------
		#设定SQL语句参数
		currentSQL=sql_yj_new.format(
			category=m_cate,
			category_zh=m_cate_zh,
			series='营业组',
			kpi_name='3万P人力',
			setMon=setMon,
			thisDateStart=dateStart,
			thisDateEnd=dateEnd,
			lastDateStart=lastDateStart,
			lastDateEnd=lastDateEnd,
			rule=30000,
			thisField='this_bf',
			lastField='last_bf',
			thisCount='agntnum',
			lastCount='agntnum',
		)
		#执行语句
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
		cur.commit()			

		#--------营销5万P人力--------------------------------------------------------------------------------
		#设定SQL语句参数
		currentSQL=sql_yj_new.format(
			category=m_cate,
			category_zh=m_cate_zh,
			series='营业组',
			kpi_name='5万P人力',
			setMon=setMon,
			thisDateStart=dateStart,
			thisDateEnd=dateEnd,
			lastDateStart=lastDateStart,
			lastDateEnd=lastDateEnd,
			rule=50000,
			thisField='this_bf',
			lastField='last_bf',
			thisCount='agntnum',
			lastCount='agntnum',
		)
		#执行语句
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
		cur.commit()			

		#--------营销10万P人力--------------------------------------------------------------------------------
		#设定SQL语句参数
		currentSQL=sql_yj_new.format(
			category=m_cate,
			category_zh=m_cate_zh,
			series='营业组',
			kpi_name='10万P人力',
			setMon=setMon,
			thisDateStart=dateStart,
			thisDateEnd=dateEnd,
			lastDateStart=lastDateStart,
			lastDateEnd=lastDateEnd,
			rule=100000,
			thisField='this_bf',
			lastField='last_bf',
			thisCount='agntnum',
			lastCount='agntnum',
		)
		#执行语句
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
		cur.commit()			

		#--------营销人力--------------------------------------------------------------------------------
		#设定SQL语句参数
		currentSQL=sql_rl.format(
			category=m_cate,
			category_zh=m_cate_zh,
			series='营业组',
			kpi_name='月初人力',
			selectFieled='rl',
			setMon=setMon,
			thisDateStart=dateStart,
			thisDateEnd=dateStart,
			lastDateStart=lastDateStart,
			lastDateEnd=lastDateStart,
		)
		#执行语句
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
		cur.commit()			
		
		#--------营销人力--------------------------------------------------------------------------------
		#设定SQL语句参数
		currentSQL=sql_rl.format(
			category=m_cate,
			category_zh=m_cate_zh,
			series='营业组',
			kpi_name='当前人力',
			selectFieled='rl',
			setMon=setMon,
			thisDateStart=dateEnd +1,
			thisDateEnd=dateEnd +1,
			lastDateStart=lastDateStart,
			lastDateEnd=lastDateStart,
		)
		#执行语句
		for curr_sql in currentSQL.split(";"):
			cur.execute(curr_sql.decode('utf-8'))
		cur.commit()			
		
		#----------------------------------------------------------------------------------------------
		#设定SQL语句参数
		currentSQL=sql_rl.format(
			category=m_cate,
			category_zh=m_cate_zh,
			series='营业组',
			kpi_name='新增人力',
			selectFieled='newrl',
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

		#--------------------------------------------------------------	
		currentSQL=sql_yj_new_zg.format(
			category=m_cate,
			category_zh=m_cate_zh,
			series='营业组',
			kpi_name='主管3千P人力',
			setMon=setMon,
			thisDateStart=dateStart,
			thisDateEnd=dateEnd,
			lastDateStart=lastDateStart,
			lastDateEnd=lastDateEnd,
			rule=3000,
			thisField='this_bf',
			lastField='last_bf',
			thisCount='agntnum',
			lastCount='agntnum',
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
dateStart -= 100
dateEnd   -= 100
lastDateStart -= 100
lastDateEnd   -= 100
#删除当日业绩
print("删除当日业绩")
cur.execute("delete from administrator.hn_kpi_information where kpi_mon>=decimal(current date -2 days)".decode('utf-8')) 
cur.commit()
print("done")

for m_category in category:
	m_cate=m_category[0]
	m_cate_zh=m_category[1]
	
	#设定SQL语句参数
	currentSQL=sql_yj.format(
		category=m_cate,
		category_zh=m_cate_zh,
		series='营业组',
		kpi_name='规模保费',
		selectFieled='gmbf_range',
		setMon=currentDate,
		thisDateStart=currentDate,
		thisDateEnd=currentDate,
		lastDateStart=lastCurrentDate,
		lastDateEnd=lastCurrentDate,
		rangeStart=-50000000,
		rangeEnd=50000000,
	)
	#执行语句
	for curr_sql in currentSQL.split(";"):
		cur.execute(curr_sql.decode('utf-8'))
	cur.commit()			
	
	#--------营销标准保费--------------------------------------------------------------------------------
	#设定SQL语句参数
	currentSQL=sql_yj.format(
		category=m_cate,
		category_zh=m_cate_zh,
		series='营业组',
		kpi_name='标准保费',
		selectFieled='bzbf_range',
		setMon=currentDate,
		thisDateStart=currentDate,
		thisDateEnd=currentDate,
		lastDateStart=lastCurrentDate,
		lastDateEnd=lastCurrentDate,
		rangeStart=-50000000,
		rangeEnd=50000000,
	)
	#执行语句
	for curr_sql in currentSQL.split(";"):
		cur.execute(curr_sql.decode('utf-8'))
	cur.commit()			

	#----------------------------------------------------------------------------------------
	#设定SQL语句参数
	currentSQL=sql_yj.format(
		category=m_cate,
		category_zh=m_cate_zh,
		series='营业组',
		kpi_name='价值保费',
		selectFieled='jzbf_range',
		setMon=currentDate,
		thisDateStart=currentDate,
		thisDateEnd=currentDate,
		lastDateStart=lastCurrentDate,
		lastDateEnd=lastCurrentDate,
		rangeStart=-50000000,
		rangeEnd=50000000,
	)
	#执行语句
	for curr_sql in currentSQL.split(";"):
		cur.execute(curr_sql.decode('utf-8'))
	cur.commit()			
	
	#--------------------------------------------------------------	
	currentSQL=sql_yj_new.format(
		category=m_cate,
		category_zh=m_cate_zh,
		series='营业组',
		kpi_name='千P人力',
		setMon=currentDate,
		thisDateStart=dateStart,
		thisDateEnd=dateEnd,
		lastDateStart=lastDateStart,
		lastDateEnd=lastDateEnd,
		rule=1000,
		thisField='this_curr_yj',
		lastField='last_curr_yj',
		thisCount='this_is_new_rl',
		lastCount='last_is_new_rl',
	)
	#执行语句
	for curr_sql in currentSQL.split(";"):
		cur.execute(curr_sql.decode('utf-8'))
	cur.commit()		

	#--------------------------------------------------------------	
	currentSQL=sql_rl_current.format(
		category=m_cate,
		category_zh=m_cate_zh,
		series='营业组',
		kpi_name='千P人力',
		setMon=currentDate,
		rule=1000,
	)
	#执行语句
	for curr_sql in currentSQL.split(";"):
		cur.execute(curr_sql.decode('utf-8'))
	cur.commit()			
	
	
	#--------------------------------------------------------------	
	currentSQL=sql_rl_current.format(
		category=m_cate,
		category_zh=m_cate_zh,
		series='营业组',
		kpi_name='3千P人力',
		setMon=currentDate,
		rule=3000,
	)
	#执行语句
	for curr_sql in currentSQL.split(";"):
		cur.execute(curr_sql.decode('utf-8'))
	cur.commit()			
	
	#--------------------------------------------------------------	
	currentSQL=sql_rl_current.format(
		category=m_cate,
		category_zh=m_cate_zh,
		series='营业组',
		kpi_name='5千P人力',
		setMon=currentDate,
		rule=5000,
	)
	#执行语句
	for curr_sql in currentSQL.split(";"):
		cur.execute(curr_sql.decode('utf-8'))
	cur.commit()			
	
	#--------------------------------------------------------------	
	currentSQL=sql_rl_current.format(
		category=m_cate,
		category_zh=m_cate_zh,
		series='营业组',
		kpi_name='6千P人力',
		setMon=currentDate,
		rule=6000,
	)
	#执行语句
	for curr_sql in currentSQL.split(";"):
		cur.execute(curr_sql.decode('utf-8'))
	cur.commit()			


	#--------------------------------------------------------------	
	currentSQL=sql_rl_current.format(
		category=m_cate,
		category_zh=m_cate_zh,
		series='营业组',
		kpi_name='1万P人力',
		setMon=currentDate,
		rule=10000,
	)
	#执行语句
	for curr_sql in currentSQL.split(";"):
		cur.execute(curr_sql.decode('utf-8'))
	cur.commit()			
	
	
	#--------营销2万P人力--------------------------------------------------------------------------------
	#设定SQL语句参数
	currentSQL=sql_rl_current.format(
		category=m_cate,
		category_zh=m_cate_zh,
		series='营业组',
		kpi_name='2万P人力',
		setMon=currentDate,
		rule=20000,
	)
	#执行语句
	for curr_sql in currentSQL.split(";"):
		cur.execute(curr_sql.decode('utf-8'))
	cur.commit()			
	
	#--------营销3万P人力--------------------------------------------------------------------------------
	#设定SQL语句参数
	currentSQL=sql_rl_current.format(
		category=m_cate,
		category_zh=m_cate_zh,
		series='营业组',
		kpi_name='3万P人力',
		setMon=currentDate,
		rule=30000,
	)
	#执行语句
	for curr_sql in currentSQL.split(";"):
		cur.execute(curr_sql.decode('utf-8'))
	cur.commit()			
	
	#--------营销5万P人力--------------------------------------------------------------------------------
	#设定SQL语句参数
	currentSQL=sql_rl_current.format(
		category=m_cate,
		category_zh=m_cate_zh,
		series='营业组',
		kpi_name='5万P人力',
		setMon=currentDate,
		rule=50000,
	)
	#执行语句
	for curr_sql in currentSQL.split(";"):
		cur.execute(curr_sql.decode('utf-8'))
	cur.commit()			
	
	#--------营销10万P人力--------------------------------------------------------------------------------
	#设定SQL语句参数
	currentSQL=sql_rl_current.format(
		category=m_cate,
		category_zh=m_cate_zh,
		series='营业组',
		kpi_name='10万P人力',
		setMon=currentDate,
		rule=100000,
	)
	#执行语句
	for curr_sql in currentSQL.split(";"):
		cur.execute(curr_sql.decode('utf-8'))
	cur.commit()			


	#--------受理3000P人力--------------------------------------------------------------------------------
	#设定SQL语句参数
	currentSQL=sl_rl_curr.format(
		category=m_cate,
		category_zh=m_cate_zh,
		series='营业组',
		kpi_name='受理3千P人力',
		setMon=currentDate,
		rule=3000,
	)
	#执行语句
	for curr_sql in currentSQL.split(";"):
		cur.execute(curr_sql.decode('utf-8'))
	cur.commit()			

	#--------受理1万P人力--------------------------------------------------------------------------------
	#设定SQL语句参数
	currentSQL=sl_rl_curr.format(
		category=m_cate,
		category_zh=m_cate_zh,
		series='营业组',
		kpi_name='受理1万P人力',
		setMon=currentDate,
		rule=10000,
	)
	#执行语句
	for curr_sql in currentSQL.split(";"):
		cur.execute(curr_sql.decode('utf-8'))
	cur.commit()			

	#--------受理3万P人力--------------------------------------------------------------------------------
	#设定SQL语句参数
	currentSQL=sl_rl_curr.format(
		category=m_cate,
		category_zh=m_cate_zh,
		series='营业组',
		kpi_name='受理3万P人力',
		setMon=currentDate,
		rule=30000,
	)
	#执行语句
	for curr_sql in currentSQL.split(";"):
		cur.execute(curr_sql.decode('utf-8'))
	cur.commit()			

	#--------受理5万P人力--------------------------------------------------------------------------------
	#设定SQL语句参数
	currentSQL=sl_rl_curr.format(
		category=m_cate,
		category_zh=m_cate_zh,
		series='营业组',
		kpi_name='受理5万P人力',
		setMon=currentDate,
		rule=50000,
	)
	#执行语句
	for curr_sql in currentSQL.split(";"):
		cur.execute(curr_sql.decode('utf-8'))
	cur.commit()			




        #-------------------------------------------------------------------------------------
        currentSQL=sql_yj.format(
        	category=m_cate,
                category_zh=m_cate_zh,
                series='营业组',
                kpi_name='承保件数',
                selectFieled='js_range',
		setMon=currentDate,
		thisDateStart=currentDate,
		thisDateEnd=currentDate,
		lastDateStart=lastCurrentDate,
		lastDateEnd=lastCurrentDate,
                rangeStart=-50000000,
                rangeEnd=50000000,
       	)
       	#执行语句
        for curr_sql in currentSQL.split(";"):
        	cur.execute(curr_sql.decode('utf-8'))
        cur.commit()


#执行合计-------------------------------------------------------------------------------------------------------
currentSQL=sql_sum.format(kpi_name='规模保费')
for curr_sql in currentSQL.split(";"):
	cur.execute(curr_sql.decode('utf-8'))
cur.commit()			

currentSQL=sql_sum.format(kpi_name='标准保费')
for curr_sql in currentSQL.split(";"):
	cur.execute(curr_sql.decode('utf-8'))
cur.commit()			

currentSQL=sql_sum.format(kpi_name='活动人力')
for curr_sql in currentSQL.split(";"):
	cur.execute(curr_sql.decode('utf-8'))
cur.commit()			

currentSQL=sql_sum.format(kpi_name='月初人力')
for curr_sql in currentSQL.split(";"):
	cur.execute(curr_sql.decode('utf-8'))
cur.commit()			

currentSQL=sql_sum.format(kpi_name='当前人力')
for curr_sql in currentSQL.split(";"):
	cur.execute(curr_sql.decode('utf-8'))
cur.commit()			

currentSQL=sql_sum.format(kpi_name='脱落人力')
for curr_sql in currentSQL.split(";"):
	cur.execute(curr_sql.decode('utf-8'))
cur.commit()			

currentSQL=sql_sum.format(kpi_name='千P人力')
for curr_sql in currentSQL.split(";"):
	cur.execute(curr_sql.decode('utf-8'))
cur.commit()			

currentSQL=sql_sum.format(kpi_name='3千P人力')
for curr_sql in currentSQL.split(";"):
	cur.execute(curr_sql.decode('utf-8'))
cur.commit()			

currentSQL=sql_sum.format(kpi_name='1万P人力')
for curr_sql in currentSQL.split(";"):
	cur.execute(curr_sql.decode('utf-8'))
cur.commit()			

currentSQL=sql_sum.format(kpi_name='2万P人力')
for curr_sql in currentSQL.split(";"):
	cur.execute(curr_sql.decode('utf-8'))
cur.commit()			

currentSQL=sql_sum.format(kpi_name='3万P人力')
for curr_sql in currentSQL.split(";"):
	cur.execute(curr_sql.decode('utf-8'))
cur.commit()			

currentSQL=sql_sum.format(kpi_name='5万P人力')
for curr_sql in currentSQL.split(";"):
	cur.execute(curr_sql.decode('utf-8'))
cur.commit()			

currentSQL=sql_sum.format(kpi_name='10万P人力')
for curr_sql in currentSQL.split(";"):
	cur.execute(curr_sql.decode('utf-8'))
cur.commit()			

currentSQL=sql_sum.format(kpi_name='主管人力')
for curr_sql in currentSQL.split(";"):
	cur.execute(curr_sql.decode('utf-8'))
cur.commit()			

currentSQL=sql_sum.format(kpi_name='新增人力')
for curr_sql in currentSQL.split(";"):
	cur.execute(curr_sql.decode('utf-8'))
cur.commit()			

currentSQL=sql_sum.format(kpi_name='承保件数')
for curr_sql in currentSQL.split(";"):
	cur.execute(curr_sql.decode('utf-8'))
cur.commit()			

currentSQL=sql_sum.format(kpi_name='新人承保件数')
for curr_sql in currentSQL.split(";"):
	cur.execute(curr_sql.decode('utf-8'))
cur.commit()			

currentSQL=sql_sum.format(kpi_name='新人标准保费')
for curr_sql in currentSQL.split(";"):
	cur.execute(curr_sql.decode('utf-8'))
cur.commit()			

currentSQL=sql_sum.format(kpi_name='新人活动人力')
for curr_sql in currentSQL.split(";"):
	cur.execute(curr_sql.decode('utf-8'))
cur.commit()			

currentSQL=sql_sum.format(kpi_name='新人3千P人力')
for curr_sql in currentSQL.split(";"):
	cur.execute(curr_sql.decode('utf-8'))
cur.commit()			

currentSQL=sql_sum.format(kpi_name='价值保费')
for curr_sql in currentSQL.split(";"):
	cur.execute(curr_sql.decode('utf-8'))
cur.commit()			

currentSQL=sql_sum.format(kpi_name='主管3千P人力')
for curr_sql in currentSQL.split(";"):
	cur.execute(curr_sql.decode('utf-8'))
cur.commit()			

#对表中的数据进行特殊处理---------------------------------------------------------------------------------------
cur.execute("update administrator.hn_kpi_information set partname='无头部' where series='营业组' and partname is null ".decode('utf-8'))
cur.execute("update administrator.hn_kpi_information set teamname=\'无头组\' where series=\'营业组\' and teamname is null ".decode('utf-8'))
cur.commit()			


#加工营业部-----------------------------------------------------------------------------------------------------
print("加工营业部")
cur.execute("delete from administrator.hn_kpi_information where series='营业部'".decode('utf-8'))
curr_sql="""
insert into administrator.hn_kpi_information 
select 
        category,
        '营业部',
        branch,
        aracde,
        partname,
        '',
        kpi_name,
        kpi_mon,
        sum(this_mon),
        sum(last_mon),
        processing_time
        from administrator.hn_kpi_information
                where series='营业组'
                        group by         
                        category,
                        branch,
                        aracde,
                        partname,
                        kpi_name,
                        kpi_mon,
                        processing_time
"""
cur.execute(curr_sql.decode('utf-8')) 
cur.commit()			


#加工支公司-----------------------------------------------------------------------------------------------------
print("加工支公司")
cur.execute("delete from administrator.hn_kpi_information where series='支公司'".decode('utf-8'))
curr_sql="""
insert into administrator.hn_kpi_information 
select 
        category,
        '支公司',
        branch,
        aracde,
        '',
        '',
        kpi_name,
        kpi_mon,
        sum(this_mon),
        sum(last_mon),
        processing_time
        from administrator.hn_kpi_information
                where series='营业组'
                        group by         
                        category,
                        branch,
                        aracde,
                        kpi_name,
                        kpi_mon,
                        processing_time
"""
cur.execute(curr_sql.decode('utf-8')) 
cur.commit()			


#加工中支-----------------------------------------------------------------------------------------------------
print("加工中支")
cur.execute("delete from administrator.hn_kpi_information where series='中支'".decode('utf-8'))
curr_sql="""
insert into administrator.hn_kpi_information 
select 
        category,
        '中支',
        branch,
	'',
        '',
        '',
        kpi_name,
        kpi_mon,
        sum(this_mon),
        sum(last_mon),
        processing_time
        from administrator.hn_kpi_information
                where series='营业组'
                        group by         
                        category,
                        branch,
                        kpi_name,
                        kpi_mon,
                        processing_time
"""
cur.execute(curr_sql.decode('utf-8')) 
cur.commit()			

#加工个险
print("加工个险")
cur.execute("delete from administrator.hn_kpi_information where category='个险'".decode('utf-8'))
curr_sql="""
insert into  administrator.hn_kpi_information
select 
        '个险',
        series,
        branch,
        aracde,
        partname,
        teamname,
        kpi_name,
        kpi_mon,
        sum(case when this_mon is null then 0 else this_mon end),
        sum(case when last_mon is null then 0 else last_mon end),
        current timestamp
        from administrator.hn_kpi_information
		where category in('营销','收展')
                        group by         
                        series,
                        branch,
                        aracde,
                        partname,
                        teamname,
                        kpi_name,
                        kpi_mon
"""
cur.execute(curr_sql.decode('utf-8')) 
cur.commit()			
cur.close()

