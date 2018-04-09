#!/bin/sh
#定义命令路径
ph='/home/db2inst/sqllib/bin'

#引入用户环境变量
source /home/db2inst/.bash_profile

#开始计时
start=`date +%s`

#开启跟踪
set -x

#函数
function checkState(){
	if [ $# -lt 2 ]; then
		echo "请输入2参数！"
		return 1
	fi
	case $2 in
		1)
			message="连接总公司数据库连续10次失败！"
			;;
		2)
			message="定义游标失败，10秒后重试！"
			;;
		3)	
			message="数据存在差异，10秒后重新加工！"
			;;
		4)
			message="数据正常!"
			;;
		*)
			message="错误内容未定义。"
			;;
	esac
	[ -z $run_time ] && run_time=0 
	$ph/db2 "insert into info.taskmessage values ('$1',current timestamp,'$message',$run_time)"
}

#连接本地库

#判断依赖表是否加工完成
isSuccess="N"
while [ $isSuccess = "N" ]
do
	sql="select case when count(*)>=2 then 'Y' else 'N' end
        	from info.taskmessage
                	where date(info_time)=current date and message='数据正常!' and tablename in('agntinfo','acctinfo')"
	db2 "select * from ($sql)a" |sed -n '4p'|awk '{print $1}' >agnt_top
	while read agnt_top
	do
        	isSuccess=$agnt_top
	done <agnt_top
	rm agnt_top
	#判断是否一致
	if [ $isSuccess = "Y" ]; then
		echo "依赖表加工成功"
	else
		sleep 20
	fi
done

#更新数据
isSuccess="N"
while [ $isSuccess = "N" ]
do
  	db2 "alter table administrator.hn_agnt_top activate not logged initially with empty table"
	db2 "INSERT INTO administrator.hn_agnt_top
	SELECT
		CASE
			WHEN agtype IN(
				'TR',
				'TB'
			) THEN 'DX'
			ELSE 'GX'
		END AS team,
		branch,
		aracde,
		INT(
			trandate / 100
		) AS mon,
		agntnum,
		substr(agntname,1,20),
		SUM( acctamt_std )/10000 AS bf,
		COUNT( DISTINCT CASE WHEN acctamt > 0 THEN chdrnum END ) - COUNT( DISTINCT CASE WHEN acctamt <= 0 THEN chdrnum END ) AS js
	FROM
		administrator.hn_acctinfo
	WHERE
		agtype IN(
			'TA',
			'SA',
			'TS',
			'SM',
			'SD',
			'SE',
			'AS',
			'SS',
			'UM',
			'AD',
			'HD',
			'AC',
			'DL',
			'PA',
			'RC',
			'GB',
			'TR',
			'TB'
		)
		AND trandate between 20170101 and 20171231
	GROUP BY
		CASE
			WHEN agtype IN(
				'TR',
			'TB'
		) THEN 'DX'
		ELSE 'GX'
	END,
	branch,
	aracde,
	INT(
		trandate / 100
	),
	agntnum,
	agntname"
	db2 "INSERT INTO administrator.hn_agnt_top
	SELECT
		CASE
			WHEN series IN(
				'TZ',
				'TZF'
			) THEN 'TZ'
			ELSE 'SQ'
		END AS team,
		branch,
		'' AS aracde,
		INT(
			trandate / 100
		) AS mon,
		agntnum,
		substr(agntname,1,20),
		SUM(case when series in('TZ','TZF') then acctamt_std else ape end)/10000 AS bf,
		COUNT( DISTINCT CASE WHEN ape > 0 THEN chdrnum END ) - COUNT( DISTINCT CASE WHEN ape < 0 THEN chdrnum END ) AS js
	FROM
		v.IPE_ACCT_BNK_KPI
	WHERE
		trandate between 20170101 and 20171231
		AND bnk_cls = '高价值'
	GROUP BY
		CASE
			WHEN series IN(
				'TZ',
				'TZF'
			) THEN 'TZ'
			ELSE 'SQ'
		END,
		branch,
		'',
		INT(
			trandate / 100
		),
		agntnum,
		agntname "

	#判断是否成功
	if [ $? = 0 ]; then
		isSuccess="Y"
		#记录结束时间
		end=`date +%s`
		#计算时间差
	        run_time=$[ end - start ]
		checkState agnt_top 4
	else
		checkState agnt_top 3
		sleep 10
	fi
done

#断开连接
$ph/db2 connect reset

#关闭跟踪
set +x
