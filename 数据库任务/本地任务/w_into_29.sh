#!/bin/sh
# 脚本名称:w_into_29.sh
# 作者：谢甲防
# 开发日期：2017-2-7
# 主功功能：加工件数表，人力表
# 数据源：10.19.2.29/hnii
# 涉及的表：table_info,hn_gx_rl,hn_gx_js,hn_acctinfo,hn_agntinfo
# 最近更新时间：2017-2-7 17:25
#
#
#--------------------------global-------------------------------------------------------

#引入用户环境变量
source /home/db2inst/.bash_profile

db2 "connect to hnii29 user administrator using 'Win2003OS)@('"; 
sql_js="
	SELECT
	series,
	branch,
	aracde,
	agntnum,
	chdrnum,
	mon,
	sum(acctamt_std) AS bf,
	js_flag,
	ismanager
	from
	(SELECT
	CASE WHEN agtype NOT in('RC') THEN '营销' ELSE '收展' END AS series,
	BRANCH,
	ARACDE,
	agntnum,
	CHDRNUM,
	INT(TRANDATE/100) AS MON,
	ACCTAMT_STD,
	CASE WHEN batc_type='CB' THEN 1 WHEN batc_type='CD' THEN -1 WHEN batc_type='FH' THEN -1 ELSE 0 END AS js_flag,
	agtype,
	CASE WHEN agtype in('AS','SS','UM','AD') THEN 1 WHEN (SELECT count(*) FROM administrator.hn_zcllinfo b WHERE a.agntnum=b.zcllctor AND a.agtype='RC' AND (b.zcllcls LIKE 'SZB%' OR b.zcllcls LIKE 'SZC%' OR b.zcllcls LIKE 'SZD%'))>0 THEN 1 ELSE 0 END as isManager
	FROM administrator.hn_acctinfo a
	WHERE trandate BETWEEN decimal(current date -day(current date -1 day) days) AND decimal(current date)
	and agntnum <>'10C00000'
	AND agtype in('TA','SA','TS','SM','SD','AS','SS','UM','AD','SE','HD','RC'))a
	GROUP BY    series,
	branch,
	aracde,
	agntnum,
	chdrnum,
	mon,
	js_flag,
	ismanager
"

#-----------------------件数-----------------------------------------------------------
kpi_list=(
'总件数'
'3千P件数'
'6千P件数'
'1万P件数'
'3万P件数'
'5万P件数'
'10万P件数'
'主管万P件数'
)

#加工数据
for kpi in ${kpi_list[*]}
do
    #判断保费标准
    case $kpi in
    '总件数')
        sql_case=""
	;;
    '3千P件数')
        sql_case=" where bf>=3000"
	;;
    '6千P件数')
        sql_case=" where bf>=6000"
	;;
    '1万P件数')
        sql_case=" where bf>=10000"
	;;
    '3万P件数')
        sql_case=" where bf>=30000"
	;;
    '5万P件数')
        sql_case=" where bf>=50000"
	;;
    '10万P件数')
        sql_case=" where bf>=100000"
	;;
    '主管万P件数')
        sql_case=" where bf>=10000 and ismanager=1"
	;;
    *)
        sql_case=""
	;;
    esac
    #提示信息
    echo "$kpi"

    #删除当月的数据
    db2 "delete from administrator.hn_gx_js where kpi_mon=int(decimal(current date)/100) and kpi_name = '$kpi' "
    #插入数据
    db2 "
    insert into administrator.hn_gx_js(series,branch,aracde,kpi_mon,kpi_name,kpi_data)
    SELECT
    series,
    branch,
    aracde,
    mon,
    '$kpi',
    sum(js_flag)
    FROM ($sql_js)a 
    $sql_case
    GROUP BY series,branch,aracde,mon
    "
done

#-----------------------万元件人力-----------------------------------------------------------
kpi_list=(
'万元件人力'
'万元件主管人力'
)

#加工数据
for kpi in ${kpi_list[*]}
do
    #判断保费标准
    case $kpi in
    '万元件人力')
        sql_case=""
	;;
    '万元件主管人力')
        sql_case=" and ismanager=1"
	;;
    *)
        sql_case=""
	;;
    esac
    #提示信息
    echo "$kpi"

    #删除当月的数据
    db2 "delete from administrator.hn_gx_rl where kpi_mon=int(decimal(current date)/100) and kpi_name = '$kpi' "
    #插入数据
    db2 "
    insert into administrator.hn_gx_rl(series,branch,aracde,kpi_mon,kpi_name,kpi_data)
    SELECT
    series,
    branch,
    aracde,
    mon,
    '$kpi',
    count(distinct agntnum)
    FROM ($sql_js)a 
    where bf>=10000
    $sql_case
    GROUP BY series,branch,aracde,mon
    "
done


#-----------------------活动人力-----------------------------------------------------------
sql_rl="
	SELECT
	series,
	branch,
	aracde,
	mon,
	agntnum,
	ismanager,
	sum(bf) AS bf,
	sum(js_flag) AS js
	FROM ($sql_js)a
	GROUP BY   
	series,
	branch,
	aracde,
	mon,
	agntnum,
	ismanager
"

kpi_list=(
'3千P人力'
'6千P人力'
'3件人力'
)

#加工数据
for kpi in ${kpi_list[*]}
do
    #判断保费标准
    sql_case=""
    case $kpi in
    '3千P人力')
        bf=3000
	;;
    '6千P人力')
        bf=6000
	;;
    '3件人力')
        bf=3000
	sql_case=" and js>=3"
	;;
    *)
        bf=0
	;;
    esac
    #提示信息
    echo "$kpi"

    #删除当月的数据
    db2 "delete from administrator.hn_gx_rl where kpi_mon=int(decimal(current date)/100) and kpi_name = '$kpi' "
    #插入数据
    db2 "
    insert into administrator.hn_gx_rl(series,branch,aracde,kpi_mon,kpi_name,kpi_data)
    SELECT
    series,
    branch,
    aracde,
    mon,
    '$kpi',
    count(distinct agntnum)
    from ($sql_rl) a
      where bf>=$bf
      $sql_case
      group by series,branch,aracde,mon
    "
done
        
#-----------------------在册人力-----------------------------------------------------------
sql_rl="
	SELECT
	CASE WHEN agtype NOT in('RC') THEN '营销' ELSE '收展' END AS series,
	branch,
	aracde,
	agntnum,
	CASE WHEN agtype in('AS','SS','UM','AD') THEN 1 WHEN (SELECT count(*) FROM administrator.hn_zcllinfo b WHERE a.agntnum=b.zcllctor AND a.agtype='RC' AND (b.zcllcls LIKE 'SZB%' OR b.zcllcls LIKE 'SZC%' OR b.zcllcls LIKE 'SZD%'))>0 THEN 1 ELSE 0 END as isManager
	FROM administrator.hn_agntinfo a
	WHERE dtetrm=99999999
	AND agtype in('TA','SA','TS','SM','SD','AS','SS','UM','AD','SE','HD','RC')
"

kpi_list=(
'在册人力'
'在册主管人力'
)

#加工数据
for kpi in ${kpi_list[*]}
do
    #判断保费标准
    case $kpi in
    '在册主管人力')
        sql_case=" where ismanager=1"
	;;
    *)
        sql_case=""
	;;
    esac
    #提示信息
    echo "$kpi"

    #删除当月的数据
    db2 "delete from administrator.hn_gx_rl where kpi_mon=int(decimal(current date)/100) and kpi_name = '$kpi' "
    #插入数据
    db2 "
    insert into administrator.hn_gx_rl(series,branch,aracde,kpi_mon,kpi_name,kpi_data)
    SELECT
    series,
    branch,
    aracde,
    int(decimal(current date)/100),
    '$kpi',
    count(distinct agntnum)
    from ($sql_rl) a
      $sql_case
      group by series,branch,aracde
    "
done






db2 "connect reset"
