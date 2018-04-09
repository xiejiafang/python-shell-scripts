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


#更新数据
isSuccess="N"
getTime=0
while [ $isSuccess != "Y" ]
do
	#定义游标
	db2 "declare c1 cursor for select * from  administrator.ipe_chdrinfo_bnk"
	#防止"3"错误
	db2 "load from test.txt of del terminate into administrator.hn_ipe_chdrinfo_bnk"
	#清空表
	db2 "alter table administrator.hn_ipe_chdrinfo_bnk activate not logged initially with empty table"
	#加工数据
	db2 "load from c1 of cursor insert into administrator.hn_ipe_chdrinfo_bnk"
	#记录结束时间
        end=`date +%s`
        #计算时间差
	run_time=$[ end - start ]
	#判断是否成功
	if [ $? = 0 ]; then
		isSuccess="Y"
		#记录结束时间
        	end=`date +%s`
        	#计算时间差
	        run_time=$[ end - start ]
		checkState ipe_chdrinfo_bnk 4
	        db2 "update administrator.hn_ipe_chdrinfo_bnk set statcode='NO' where statcode='M_DEL'"
	        db2 "update administrator.hn_ipe_chdrinfo_bnk set bk_holding_name='中原银行' where bk_holding_name is null and bk_name like '%中原银行%'"
	else
		checkState ipe_chdrinfo_bnk 3
		getTime=$[getTime+1]
		if [ $getTime -eq 3 ]; then
			break
		fi
		sleep 10
	fi
done

#断开连接
$ph/db2 connect reset

#加工KPI
#exec ~/数据库任务/本地任务/kpi_info_bnk.py

#关闭跟踪
set +x
