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
	$ph/db2 "connect to tk_info user tkipe using tknew"  
	if [ $? == 0 ]; then
		isConn="Y"
		$ph/db2 "export to xuqidetail_bnk of del 
		SELECT 
		        agntbr,
		        chdrnum,
		        premium,
		        cnttype,
		        occdate,
		        instfrom,
			substr(agentnum,1,8)as agntnum,
		        medicode,
		        FLAG
		FROM ipe_banklins 
		        where company='D' 
		union         
		SELECT 
		        agntbr,
		        chdrnum,
		        premium,
		        cnttype,
		        occdate,
		        adjdate,
			substr(agentnum,1,8)as agntnum,
		        medicode,
		        FLAG
		FROM ipe_bankysys 
		        where company='D' "
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
$ph/db2 connect to hnii

#下载数据
isSuccess="N"
getTime=0
while [ $isSuccess != "Y" ]
do
	#清空表
	db2 "alter table administrator.hn_xuqidetail_bnk activate not logged initially with empty table"
	#加工数据
	db2 "load from xuqidetail_bnk of del insert into administrator.hn_xuqidetail_bnk"
	#判断是否一致
	if [ $? = 0 ]; then
		db2 "update administrator.HN_XUQIDETAIL_BNK a 
        		set branch=(select distinct c.branch from administrator.hn_basdata b,administrator.hn_branch c 
                        	where a.bm_cert=b.bm_cert and b.station=c.station )
                	where branch is null"
		isSuccess="Y"
		#记录结束时间
        	end=`date +%s`
        	#计算时间差
	        run_time=$[ end - start ]
		checkState xuqidetail_bnk 4
	else
		checkState xuqidetail_bnk 3
                getTime=$[getTime+1]
                if [ $getTime -ge 3 ]; then
                        break
                fi
		sleep 600
	fi
	rm xuqidetail_bnk
done

#断开连接
$ph/db2 connect reset

#关闭跟踪
set +x
