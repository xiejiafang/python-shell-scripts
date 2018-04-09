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
		$ph/db2 "export to all_dgx of del 
		select 
		        a.company,
		        (select shortdesc from ipe_company b where a.company=b.descitem),
		        trandate,  
		        case when agtype ='HA' then '经代' when agtype in('TR','TB') then '电销' else '个险' end as series,                 
		        sum(acctamt_std) as dgxbf
		        from v_acct_day a
		                where trandate between int(decimal(current date))/10000*10000 and decimal(current date - 1 days)
		                and agtype IN ('TA','SA','TS','SM','SD','SE','AS','SS','UM','AD','HD','AC','DL','PA','RC','TR','TB','HA')
		                --and batctrcde<>'TGJC'
		                group by 
		                        a.company,
		                        trandate,
		                        case when agtype ='HA' then '经代' when agtype in('TR','TB') then '电销' else '个险' end
		union all
		select 
		        a.company,
		        (select shortdesc from ipe_company b where a.company=b.descitem),
		        trandate,  
		        '银保' as series,      
		        sum(ape) as dgxbf
		        from ipe_acct_bnk a
		                where trandate between int(decimal(current date))/10000*10000 and decimal(current date -1 days)
		                and bnk_cls='高价值'
		                group by
		                        a.company,
                        		trandate"
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
while [ $isSuccess != "Y" ]
do
	#清空表
	db2 "alter table administrator.hn_all_dgx activate not logged initially with empty table"
	#加工数据
	db2 "load from all_dgx of del insert into administrator.hn_all_dgx"
	#判断是否一致
	if [ $? = 0 ]; then
		isSuccess="Y"
		#记录结束时间
        	end=`date +%s`
        	#计算时间差
	        run_time=$[ end - start ]
		checkState all_dgx 4
	else
		checkState all_dgx 3
                getTime=$[getTime+1]
                if [ $getTime -ge 3 ]; then
                        break
                fi
		sleep 600
	fi
	rm all_dgx
done

#断开连接
$ph/db2 connect reset

source /home/db2inst/数据库任务/本地任务/w_into_36.sh

#关闭跟踪
set +x
