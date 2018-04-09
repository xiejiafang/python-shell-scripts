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

#连接数据库
isConn="N"
time=0
while [ $isConn = "N" ]
do
	if [ $? == 0 ]; then
		isConn="Y"
		db2 connect reset
	else
		echo "连接失败，10秒后重新连接！"
		time=$[time+1]
		if [ $time -eq 10 ]; then
			checkState connect 1
			time=0
		fi
		sleep 10
	fi
done

#连接本地库

#下载数据
isSuccess="N"
getTime=0
myDays=400
while [ $isSuccess != "Y" ]
do
	#加工数据
	db2 "call p.get_basdata($myDays)"
	#核对数据
	sql="select case when a.bf=b.bf and a.js=b.js then 'Y' else 'N' end from 
		(select sum(jf_je) bf,count(distinct bm_cert) js from administrator.hn_basdata 
	        where jf_rq between current date - day(current date -1 day) days and current date 
       		and app_flag in ('1','WT') and medi_code not like '%WY%') a,
		(select sum(jf_je) bf,count(distinct bm_cert) js from administrator.ba_prd_data
	        where jf_rq between current date - day(current date -1 day) days and current date 
       		and app_flag in ('1','WT') and medi_code not like '%WY%')b"
	db2 "select * from ($sql)a" |sed -n '4p'|awk '{print $1}' >basdata
	while read basdata
	do
        	isSuccess=$basdata
	done <basdata
	rm basdata
	#判断是否一致
	if [ $isSuccess = "Y" ]; then
		#记录结束时间
        	end=`date +%s`
        	#计算时间差
	        run_time=$[ end - start ]
		checkState basdata 4
	else
		checkState basdata 3
                getTime=$[getTime+1]
                myDays=$[myDays+myDays]
                if [ $getTime -ge 3 ]; then
                        break
                fi
		sleep 60
	fi
done

#断开连接
$ph/db2 connect reset

#关闭跟踪
set +x
