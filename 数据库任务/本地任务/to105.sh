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
db2 "delete from db105.cbbf"
db2 "
	INSERT INTO DB105.CBBF  
	SELECT
	aracde,
	sum(acctamt_std)/10000
	FROM administrator.hn_acctinfo
	WHERE agtype in(select agtype from administrator.hn_agtype)
	AND trandate BETWEEN 20170101 AND decimal(current date)
	GROUP BY aracde
"

#断开连接
$ph/db2 connect reset

#关闭跟踪
set +x
