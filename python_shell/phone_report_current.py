#!/usr/bin/python
#coding:utf-8
import commands
import sys
import pyodbc

#设置数据库连接
conn=pyodbc.connect('DRIVER={DB2};SERVER=10.19.19.34;DATABASE=HNII;UID=db2inst;PWD=okm34db2&;charset=utf-8')
cur = conn.cursor()

#个险实时业绩
curr_sql= 'delete from db_33.gx_ssyj_branch '
cur.execute(curr_sql.decode('utf-8')) 
curr_sql="""
insert into db_33.gx_ssyj_branch(branch,category,curr_js,curr_bf,mon_js,mon_bf)
select 
        branch,
        category,
        value(sum(case when kpi_mon=decimal(current date) and kpi_name='承保件数' then this_mon end),0) curr_js,
        value(sum(case when kpi_mon=decimal(current date) and kpi_name='标准保费' then this_mon end)/10000,0) curr_bf,
        value(sum(case when kpi_name='承保件数' then this_mon end),0) mon_js,
        value(sum(case when kpi_name='标准保费' then this_mon end)/10000,0) mon_bf
        from administrator.hn_kpi_information 
                where series='支公司'
                and kpi_name in('标准保费','承保件数')
                and kpi_mon>=int(decimal(current date)/100)
                and branch<>'合计'
                        group by 
                        branch,
                        category
                                order by mon_bf desc
"""
cur.execute(curr_sql.decode('utf-8')) 
cur.commit()			

#未承保保单明细
curr_sql="delete from db_33.gx_chdr_ys_list"
cur.execute(curr_sql.decode('utf-8')) 
curr_sql="""
	INSERT INTO db_33.gx_chdr_ys_list(branch,branch_name,chdrnum,cnttype,sl_date,ys_date,bf)
	SELECT
	*
	FROM 
	(SELECT
	cntbranch,
	f.getbranchname(cntbranch) AS branch_name,
	chdrnum,
	cnttype,
	hprrcvdt,
	sl_date,
	(SELECT sum(acctamt) FROM administrator.hn_rtrninfo b WHERE a.chdrnum=b.chdrnum AND trandate BETWEEN decimal(current date - 3 months) AND decimal(current date) ) AS bf
	FROM administrator.hn_chdrinfo a
	WHERE sl_date BETWEEN decimal(current date - 3 months) AND decimal(current date)
	AND srcebus='AG'
	AND cb_date =99999999) a
	WHERE a.bf<>0
"""
cur.execute(curr_sql.decode('utf-8')) 
cur.commit()			




#支公司业绩
curr_sql= 'delete from db_33.gx_ssyj_aracde '
cur.execute(curr_sql.decode('utf-8')) 
curr_sql="""
insert into db_33.gx_ssyj_aracde(branch,aracde,category,curr_js,curr_bf,mon_js,mon_bf)
select 
        a.branch,
        a.aracde,
        category,
        value(sum(case when kpi_mon=decimal(current date) and kpi_name='承保件数' then this_mon end),0) curr_js,
        value(sum(case when kpi_mon=decimal(current date) and kpi_name='标准保费' then this_mon end)/10000,0) curr_bf,
        value(sum(case when kpi_name='承保件数' then this_mon end),0) mon_js,
        value(sum(case when kpi_name='标准保费' then this_mon end)/10000,0) mon_bf
        from administrator.hn_kpi_information a,administrator.hn_branch b
                where series='支公司'
                and a.branch=b.name
                and kpi_name in('标准保费','承保件数')
                and kpi_mon>=int(decimal(current date)/100)
                and a.branch<>'合计'
                        group by 
                        a.branch,
                        a.aracde,
                        category,
                        b.branch
"""
cur.execute(curr_sql.decode('utf-8')) 
curr_sql="""
UPDATE db_33.gx_ssyj_aracde SET aracde=branch||'_'||aracde
  WHERE aracde in('本部','市区')
"""
cur.execute(curr_sql.decode('utf-8')) 
cur.commit()			


#承保明细
curr_sql= 'delete from db_33.curr_chdr_list '
cur.execute(curr_sql.decode('utf-8')) 
curr_sql="""
INSERT INTO db_33.curr_chdr_list(aracde,agntname,chdrnum,cnt,bf)
SELECT
  aracde,
  agntname,
  chdrnum,
  f.getcnttypename(cnttype) AS cnt,
  sum(acctamt_std)
  FROM administrator.hn_acctinfo
    WHERE trandate=decimal(current date)
    AND agtype IN ('TA','SA','TS','SM','SD','SE','AS','SS','UM','AD','HD','AC','DL','PA','RC','TR','TB','HA')
       GROUP BY aracde,agntname,chdrnum,cnttype
"""
cur.execute(curr_sql.decode('utf-8')) 
cur.commit()			


#支公司全年排名
curr_sql= 'delete from db_33.gx_aracde_rank_year '
cur.execute(curr_sql.decode('utf-8')) 
curr_sql="""
INSERT INTO db_33.gx_aracde_rank_year(branch,aracde,task,year_bf,rate,rank,rank_on_bf)
SELECT
  a.branch,
  shortname,
  task,
  this,
  CASE WHEN task>0 THEN this*100.00/task ELSE 0 END AS rate_rank,
  ROW_NUMBER()over() AS rank_in_all,
  rank_on_bf
  FROM 
(SELECT
    f.getbranchname(branch) AS branch,
	shortname,
	sum(task_aim) AS task
FROM
	administrator.hn_task a,administrator.hn_aracde b 
WHERE
    a.branch_name=b.aracde
	AND task_name = '支公司新契约任务'
	AND task_type = '基础目标'
	AND insert_time =(
		SELECT
			MAX( insert_time )
		FROM
			administrator.hn_task
		WHERE
			task_name = '支公司新契约任务'
			AND task_type = '基础目标'
	)
	GROUP BY branch,shortname)a,
(SELECT
  branch,
  aracde,
  sum(this_mon)/10000 AS this,
  sum(last_mon)/10000 AS last,
  ROW_NUMBER()over()-1 AS rank_on_bf
  FROM administrator.hn_kpi_information
    WHERE category='个险'
    AND series='支公司'
    AND kpi_name='标准保费'
    AND kpi_mon BETWEEN year(current date)*100+1 AND decimal(current date)
      GROUP BY branch,aracde order by this desc) b 
      WHERE a.branch=b.branch
      AND a.shortname=b.aracde
        ORDER BY rate_rank desc
"""
cur.execute(curr_sql.decode('utf-8')) 
cur.commit()			

#个险当日实时业绩
curr_sql= 'delete from db_33.gx_ssyj_day '
cur.execute(curr_sql.decode('utf-8')) 
curr_sql="""
insert into db_33.gx_ssyj_day(branch,sl_js,sl_bf,ys_js,ys_bf,cb_js,cb_bf,sl_bb)
with sl as(
select 
  branch,
  count(distinct chdrnum) as zjs,
  sum(acctamt) as zbf,
  sum(acctamt_std) as zbf_bb
  from administrator.hn_ipe_hpad
    where agtype IN ('TA','SA','TS','SM','SD','SE','AS','SS','UM','AD','HD','AC','DL','PA','RC')
    and HPRRCVDT = decimal(current date -0 days)
    AND batc_type in('SL','DZ')
      group by branch),
ys as(
select 
  branch,
  count(distinct chdrnum) as zjs,
  sum(acctamt_std) as zbf
  from administrator.hn_rtrninfo
    where agtype IN ('TA','SA','TS','SM','SD','SE','AS','SS','UM','AD','HD','AC','DL','PA','RC')
    and trandate = decimal(current date -0 days)
      group by branch),
cb as(
select 
  branch,
  count(distinct chdrnum) as zjs,
  sum(acctamt_std) as zbf
  from administrator.hn_acctinfo
    where agtype IN ('TA','SA','TS','SM','SD','SE','AS','SS','UM','AD','HD','AC','DL','PA','RC')
    and trandate = decimal(current date -0 days)
      group by branch)       
select 
  value(a.name,'分公司'),
  sum(b.zjs),
  sum(b.zbf)/10000,
  sum(c.zjs),
  sum(c.zbf)/10000,
  sum(d.zjs),
  sum(d.zbf)/10000,
  sum(b.zbf_bb)/10000
  from administrator.hn_branch a left join sl b on a.branch=b.branch left join ys c on a.branch=c.branch left join cb d on a.branch=d.branch
    where a.branch<>'D'
      group by grouping sets(name,())

"""
cur.execute(curr_sql.decode('utf-8')) 
cur.commit()			

#个险当月实时业绩
curr_sql= 'delete from db_33.gx_ssyj_mon '
cur.execute(curr_sql.decode('utf-8')) 
curr_sql="""
insert into db_33.gx_ssyj_mon(branch,sl_js,sl_bf,ys_js,ys_bf,cb_js,cb_bf,sl_bb)
with sl as(
select 
  branch,
  count(distinct chdrnum) as zjs,
  sum(acctamt) as zbf,
  sum(acctamt_std) as zbf_bb
  from administrator.hn_ipe_hpad
    where agtype IN ('TA','SA','TS','SM','SD','SE','AS','SS','UM','AD','HD','AC','DL','PA','RC')
    and HPRRCVDT between int(decimal(current date)/100)*100+1 and decimal(current date -0 days)
    AND batc_type in('SL','DZ')
      group by branch),
ys as(
select 
  branch,
  count(distinct chdrnum) as zjs,
  sum(acctamt_std) as zbf
  from administrator.hn_rtrninfo
    where agtype IN ('TA','SA','TS','SM','SD','SE','AS','SS','UM','AD','HD','AC','DL','PA','RC')
    AND batc_type <>'CS'
    and trandate between int(decimal(current date)/100)*100+1 and decimal(current date -0 days)
      group by branch),
cb as(
select 
  branch,
  count(distinct chdrnum) as zjs,
  sum(acctamt_std) as zbf
  from administrator.hn_acctinfo
    where agtype IN ('TA','SA','TS','SM','SD','SE','AS','SS','UM','AD','HD','AC','DL','PA','RC')
    and trandate between int(decimal(current date)/100)*100+1 and decimal(current date -0 days)
      group by branch)       
select 
  value(a.name,'分公司'),
  sum(b.zjs),
  sum(b.zbf)/10000,
  sum(c.zjs),
  sum(c.zbf)/10000,
  sum(d.zjs),
  sum(d.zbf)/10000,
  sum(b.zbf_bb)/10000
  from administrator.hn_branch a left join sl b on a.branch=b.branch left join ys c on a.branch=c.branch left join cb d on a.branch=d.branch
    where a.branch<>'D'
      group by grouping sets(name,())

"""
cur.execute(curr_sql.decode('utf-8')) 
cur.commit()			

#个险当月实时业绩_鑫享
curr_sql= 'delete from db_33.gx_ssyj_mon_a30 '
cur.execute(curr_sql.decode('utf-8')) 
curr_sql="""
insert into db_33.gx_ssyj_mon_a30(branch,sl_js,sl_bf,ys_js,ys_bf,cb_js,cb_bf)
with sl as(
select 
  branch,
  count(distinct chdrnum) as zjs,
  sum(acctamt) as zbf
  from administrator.hn_ipe_hpad
    where agtype IN ('TA','SA','TS','SM','SD','SE','AS','SS','UM','AD','HD','AC','DL','PA','RC')
    and HPRRCVDT between int(decimal(current date)/100)*100+1 and decimal(current date -0 days)
    and cnttype = 'A30'
      group by branch),
ys as(
select 
  branch,
  count(distinct chdrnum) as zjs,
  sum(acctamt_std) as zbf
  from administrator.hn_rtrninfo
    where agtype IN ('TA','SA','TS','SM','SD','SE','AS','SS','UM','AD','HD','AC','DL','PA','RC')
    and trandate between int(decimal(current date)/100)*100+1 and decimal(current date -0 days)
    and cnttype = 'A30'
      group by branch),
cb as(
select 
  branch,
  count(distinct chdrnum) as zjs,
  sum(acctamt_std) as zbf
  from administrator.hn_acctinfo
    where agtype IN ('TA','SA','TS','SM','SD','SE','AS','SS','UM','AD','HD','AC','DL','PA','RC')
    and trandate between int(decimal(current date)/100)*100+1 and decimal(current date -0 days)
    and cnttype = 'A30'
      group by branch)       
select 
  value(a.name,'分公司'),
  sum(b.zjs),
  sum(b.zbf)/10000,
  sum(c.zjs),
  sum(c.zbf)/10000,
  sum(d.zjs),
  sum(d.zbf)/10000
  from administrator.hn_branch a left join sl b on a.branch=b.branch left join ys c on a.branch=c.branch left join cb d on a.branch=d.branch
    where a.branch<>'D'
      group by grouping sets(name,())

"""
cur.execute(curr_sql.decode('utf-8')) 
cur.commit()			

#个险当月实时业绩_健康尊享
curr_sql= 'delete from db_33.gx_ssyj_mon_a39 '
cur.execute(curr_sql.decode('utf-8')) 
curr_sql="""
insert into db_33.gx_ssyj_mon_a39(branch,sl_js,sl_bf,ys_js,ys_bf,cb_js,cb_bf)
with sl as(
select 
  branch,
  count(distinct chdrnum) as zjs,
  sum(acctamt) as zbf
  from administrator.hn_ipe_hpad
    where agtype IN ('TA','SA','TS','SM','SD','SE','AS','SS','UM','AD','HD','AC','DL','PA','RC')
    and HPRRCVDT between int(decimal(current date)/100)*100+1 and decimal(current date -0 days)
    and cnttype = 'A39'
      group by branch),
ys as(
select 
  branch,
  count(distinct chdrnum) as zjs,
  sum(acctamt_std) as zbf
  from administrator.hn_rtrninfo
    where agtype IN ('TA','SA','TS','SM','SD','SE','AS','SS','UM','AD','HD','AC','DL','PA','RC')
    and trandate between int(decimal(current date)/100)*100+1 and decimal(current date -0 days)
    and cnttype = 'A39'
      group by branch),
cb as(
select 
  branch,
  count(distinct chdrnum) as zjs,
  sum(acctamt_std) as zbf
  from administrator.hn_acctinfo
    where agtype IN ('TA','SA','TS','SM','SD','SE','AS','SS','UM','AD','HD','AC','DL','PA','RC')
    and trandate between int(decimal(current date)/100)*100+1 and decimal(current date -0 days)
    and cnttype = 'A39'
      group by branch)       
select 
  value(a.name,'分公司'),
  sum(b.zjs),
  sum(b.zbf)/10000,
  sum(c.zjs),
  sum(c.zbf)/10000,
  sum(d.zjs),
  sum(d.zbf)/10000
  from administrator.hn_branch a left join sl b on a.branch=b.branch left join ys c on a.branch=c.branch left join cb d on a.branch=d.branch
    where a.branch<>'D'
      group by grouping sets(name,())

"""
cur.execute(curr_sql.decode('utf-8')) 
cur.commit()			


#银保大个险实时业绩
curr_sql= 'delete from db_33.yb_dgx_ssyj_branch '
cur.execute(curr_sql.decode('utf-8')) 
curr_sql="""
insert into db_33.yb_dgx_ssyj_branch(branch,dayyb,daysq,daytz,monyb,monsq,montz)
select 
        value(branch,'合计') as branch,
        sum(case when category='银保' and kpi_mon=decimal(current date) then this_mon else 0 end)/10000 as dayyb,
        sum(case when category='首期' and kpi_mon=decimal(current date) then this_mon else 0 end)/10000 as daysq,
        sum(case when category='拓展' and kpi_mon=decimal(current date) then this_mon else 0 end)/10000 as daytz,
        sum(case when category='银保' then this_mon else 0 end)/10000 as yb,
        sum(case when category='首期' then this_mon else 0 end)/10000 as sq,
        sum(case when category='拓展' then this_mon else 0 end)/10000 as tz
        from administrator.hn_kpi_info_bnk
                where kpi_name='APE保费'
                and kpi_mon>=int(decimal(current date)/100)
                and series='中支'
                        group by grouping sets((branch),())
"""
cur.execute(curr_sql.decode('utf-8')) 
cur.commit()			

#银保大个险任务达成
curr_sql= 'delete from db_33.yb_dgx_rate_branch '
cur.execute(curr_sql.decode('utf-8')) 
curr_sql="""
insert into db_33.yb_dgx_rate_branch(branch,category,mon_task,mon_bf,year_task,year_bf)
select 
  a.branch,
  a.category,
  b.mon_task,
  a.mon_bf,
  b.year_task,
  a.year_bf
  from
(select 
    branch,
    category,
    sum(case when kpi_mon>=int(decimal(current date)/100) then this_mon end)/10000 as mon_bf,
    sum(this_mon)/10000 as year_bf
    from administrator.hn_kpi_info_bnk
        where kpi_name='APE保费'
            and kpi_mon>=year(current date)*100+1
            and series='中支'
                group by branch,category) a left join 
(select 
    case when branch_name in('TZF') then 'FIC' else branch_name end as branch,
    case when task_name='银保首期大个险任务' then '首期' when task_name='银保拓展大个险任务' then '拓展' else '银保' end as category,
    sum(case when task_mon=int(decimal(current date)/100) then task_aim end) as mon_task,
    sum(task_aim) as year_task
    from administrator.hn_task
        where task_name in('银保首期大个险任务','银保拓展大个险任务','银保大个险_ALL')
        and task_type='追踪任务'
        and insert_time in(select max(insert_time) from administrator.hn_task
                             where task_name in('银保首期大个险任务','银保拓展大个险任务','银保大个险_ALL')
                             and task_type='追踪任务' group by task_name)                           
            group by branch_name,task_name) b on a.branch=b.branch and a.category=b.category
"""

cur.execute(curr_sql.decode('utf-8')) 
cur.commit()			

#个险人力指标
curr_sql= 'delete from db_33.gx_rl_kpi_branch '
cur.execute(curr_sql.decode('utf-8')) 
curr_sql="""
insert into db_33.gx_rl_kpi_branch(branch,category,kpi_name,rl,curr_rl)
select 
        branch,
        category,
        kpi_name,
        sum(this_mon) as rl,
        sum(case when kpi_mon=decimal(current date) then this_mon end) as rl_curr
        from administrator.hn_kpi_information
                where kpi_name in('月初人力','千P人力','3千P人力','1万P人力','6千P人力','1万P人力','3万P人力','5万P人力','受理3千P人力','受理1万P人力','受理3万P人力','受理5万P人力')
                and kpi_mon >= int(decimal(current date)/100)
                and series='中支'
                and branch<>'合计'
                        group by branch,category,kpi_name	
"""
cur.execute(curr_sql.decode('utf-8')) 
cur.commit()			

#个险人力指标
curr_sql= 'delete from db_33.gx_allwyj_branch '
cur.execute(curr_sql.decode('utf-8')) 
curr_sql="""
insert into db_33.gx_allwyj_branch(category,branch,rl,rl12,rl30,rl50)
with rl as(
select 
  case when agtype='RC' then 'SZ' else 'YX' end as category,
  branch,
  count(agntnum) as rl
  from administrator.hn_agntinfo
    where dtetrm=99999999
    and agtype in ('RC','TS','TA','SA','SM','SD','AS','SS','UM','AD','SE','HD')
      group by branch,case when agtype='RC' then 'SZ' else 'YX' end),
yj as(      
select 
  case when agtype='RC' then 'SZ' else 'YX' end as category,
  branch,
  count(distinct case when bf>=15000 then agntnum end) as rl12,
  count(distinct case when bf>=35000 then agntnum end) as rl30,
  count(distinct case when bf>=50000 then agntnum end) as rl50
  from (
        select 
          a.branch,
          a.agntnum,
          a.agtype,
          chdrnum,
          sum(acctamt_std) as bf
          from administrator.hn_acctinfo a,administrator.hn_agntinfo b
            where trandate between decimal(current date - day(current date -1 day) days) and decimal(current date)
            and a.agntnum=b.agntnum 
            and a.agtype in ('RC','TS','TA','SA','SM','SD','AS','SS','UM','AD','SE','HD')
            group by 
                  a.branch,
                  a.agntnum,
                  chdrnum,
                  a.agtype
                    having sum(acctamt_std)>=10000) a
       group by branch,case when agtype='RC' then 'SZ' else 'YX' end )
select 
  a.category,
  f.getbranchname(a.branch),
  value(a.rl,0) as rl,
  value(b.rl12,0) as rl12,
  value(b.rl30,0) as rl30,
  value(b.rl50,0) as rl50
  from rl a left join yj b on a.branch=b.branch and a.category=b.category  
"""
cur.execute(curr_sql.decode('utf-8')) 
cur.commit()			


#各职级人力分布
curr_sql= 'delete from db_33.gx_rl_map '
cur.execute(curr_sql.decode('utf-8')) 
curr_sql="""
insert into db_33.gx_rl_map(agtype,this_rl,last_rl)
with rl as(
select 
  a.branch,
  a.aracde,
  a.agntnum,
  case when a.agtype='RC' then a.xqtype else a.agtype end as agtype,
  mon  
  from administrator.hn_agnt_his_simpleness a,administrator.hn_agntinfo b
    where a.agtype in ('TA','SA','TS','SM','SD','SE','AS','SS','UM','AD','HD','RC')  
    and a.agntnum=b.agntnum
    and mon in(year(current date -1 day)*100+month(current date -1 day),year(current date -1 year)*100+month(current date -1 year))
    and status='Y'),  
process as(    
select 
  case 
    when agtype like '%SZB%' or agtype like '%SZC%' or agtype like '%SZD%' then 'SZ' 
    when length(agtype)=4 then 'RC' 
    when agtype in('SD','SM','SE') then '其它' 
    when agtype is null then '其它' else agtype end as agtype,
  count(case when mon=year(current date -1 day)*100+month(current date -1 day) then agntnum end) as this_rl,
  count(case when mon=year(current date -1 year)*100+month(current date -1 year) then agntnum end) as last_rl
    from rl
      group by 
        case 
    when agtype like '%SZB%' or agtype like '%SZC%' or agtype like '%SZD%' then 'SZ' 
    when length(agtype)=4 then 'RC' 
    when agtype in('SD','SM','SE') then '其它'
    when agtype is null then '其它' else agtype end 
      order by case when agtype ='AD' then 1 
        when agtype ='UM' then 2 
        when agtype ='SS' then 3
        when agtype ='AS' then 4
        when agtype ='TS' then 5
        when agtype ='SA' then 6
        when agtype ='TA' then 7
        when agtype ='SZ' then 8
        when agtype ='RC' then 9
        ELSE 10 END 
        )
select 
  value(agtype,'合计') as agtype,
  sum(this_rl) this,
  sum(last_rl) last
  from process   
    group by grouping sets(agtype,())
"""
cur.execute(curr_sql.decode('utf-8')) 
cur.commit()			


#净增人力
curr_sql= 'delete from db_33.gx_rl_analysis '
cur.execute(curr_sql.decode('utf-8')) 
curr_sql="""
insert into db_33.gx_rl_analysis(branch,curr_rl,new_rl,del_rl,pure_rl)
SELECT 
  branch,
  sum(case when kpi_name='当前人力' then this_mon end) AS current_rl,
  sum(case when kpi_name='新增人力' then this_mon end) AS new_rl,
  sum(case when kpi_name='脱落人力' then this_mon end) AS del_rl,
  sum(case when kpi_name='新增人力' then this_mon end) - sum(case when kpi_name='脱落人力' then this_mon end) AS pure_rl
  FROM administrator.hn_kpi_information
    WHERE kpi_name in('当前人力','新增人力','脱落人力')
    AND kpi_mon=int(decimal(current date)/100)
    AND series = '中支'
    AND category='个险'
      GROUP BY branch
        ORDER BY CURRENT_rl desc
"""
cur.execute(curr_sql.decode('utf-8')) 
cur.commit()			


#净增人力
curr_sql= 'delete from db_33.gx_a46 '
cur.execute(curr_sql.decode('utf-8')) 
curr_sql="""
INSERT INTO db_33.gx_a46(branch,series,sljs,slbf,cbjs,cbbf)
with sl as(
SELECT 
  branch,
  agtype,
  sum(zjs) AS zjs,
  sum(zbf) AS zbf 
  FROM (
select
  branch,
  CASE WHEN agtype='RC' THEN 'RC' ELSE 'YX' END AS agtype,
  count(distinct chdrnum) as zjs,
  sum(acctamt_std) as zbf
  from administrator.v_ycb_hpad
    where agtype IN ('TA','SA','TS','SM','SD','SE','AS','SS','UM','AD','HD','AC','DL','PA','RC')
    and HPRRCVDT between int(decimal(current date)/100)*100+1 and decimal(current date -0 days)
    and cnttype = 'A46'
      group by branch,CASE WHEN agtype='RC' THEN 'RC' ELSE 'YX' END
union
select
  branch,
  CASE WHEN agtype='RC' THEN 'RC' ELSE 'YX' END AS agtype,
  count(distinct chdrnum) as zjs,
  sum(acctamt_std) as zbf
  from administrator.v_ycb_hpad_day
    where agtype IN ('TA','SA','TS','SM','SD','SE','AS','SS','UM','AD','HD','AC','DL','PA','RC')
    and HPRRCVDT between int(decimal(current date)/100)*100+1 and decimal(current date -0 days)
    and cnttype = 'A46'
      group by branch,CASE WHEN agtype='RC' THEN 'RC' ELSE 'YX' END)
      GROUP BY branch,agtype
      ),
cb as(
select
  branch,
  CASE WHEN agtype='RC' THEN 'RC' ELSE 'YX' END AS agtype,
  count(distinct chdrnum) as zjs,
  sum(acctamt_std) as zbf
  from administrator.hn_acctinfo
    where agtype IN ('TA','SA','TS','SM','SD','SE','AS','SS','UM','AD','HD','AC','DL','PA','RC')
    and trandate between int(decimal(current date)/100)*100+1 and decimal(current date -0 days)
    and cnttype = 'A46'
      group by branch,CASE WHEN agtype='RC' THEN 'RC' ELSE 'YX' END)
select
  a.name,
  value(b.agtype,c.agtype),
  value(sum(b.zjs),0),
  value(sum(b.zbf)/10000,0) AS slbf,
  value(sum(c.zjs),0),
  value(sum(c.zbf)/10000,0)
  from administrator.hn_branch a left join sl b on a.branch=b.branch left join cb c on a.branch=c.branch
    where a.branch<>'D'
      group BY name,value(b.agtype,c.agtype)

cur.execute(curr_sql.decode('utf-8')) 
"""




















cur.commit()			
cur.close()

	
