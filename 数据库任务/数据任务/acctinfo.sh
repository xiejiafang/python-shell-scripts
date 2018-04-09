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
		5)
			message="缺少数据!"
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
	sql="select case when count(*)>=1 then 'Y' else 'N' end
        	from info.taskmessage
                	where date(info_time)=current date and message='数据正常!' and tablename='agntinfo'"
	isSuccess=$(db2 -x "$sql")
	#判断是否一致
	if [ $isSuccess = "Y" ]; then
		echo "依赖表加工成功"
	else
		sleep 20
	fi
done
#下载数据
isSuccess="N"
getTimes=0
myDays=10
while [ $isSuccess = "N" ]
do
	#加工数据
	db2 "call p.get_acctinfo($myDays)"
	db2 "update administrator.hn_acctinfo a 
		set (branch,aracde,partnum,teamnum)=(SELECT branch,aracde,partnum,teamnum FROM administrator.hn_agntinfo b WHERE a.AGNTNUM=b.AGNTNUM) 
	        where trandate between decimal(current date - $myDays days) and decimal(current date)"
	db2 "update administrator.hn_acctinfo set (branch,aracde)=('80','807') where trandate between decimal(current date - $myDays days) and decimal(current date) and branch='I0'" 
	db2 "update administrator.hn_acctinfo a
		set fyc=acctamt_std*(select rate from administrator.hn_fyc_rate b where a.inss=b.crtable and a.freq=b.freq and a.period=b.period) 
		where a.trandate between decimal(current date - $myDays days) and decimal(current date) "
	#核对数据
	sql="select case when a.js=b.js then 'Y' else 'N' end from 
		(select sum(acctamt_std) bf,count(distinct chdrnum) js from administrator.hn_acctinfo 
	        where trandate between decimal(current date - day(current date -1 day) days) and decimal(current date -1 days) 
       		and batctrcde<>'TGJC') a,
		(select sum(acctamt_std) bf,count(distinct chdrnum) js from (select distinct * from administrator.ipe_acct
	        where trandate between decimal(current date - day(current date -1 day) days) and decimal(current date -1 days) 
		and batctrcde<>'BA67')b) b"
	isSuccess=$(db2 -x "select * from ($sql)a" )
	#判断是否一致
	if [ $isSuccess = "Y" ]; then
		#记录结束时间
        	end=`date +%s`
        	#计算时间差
	        run_time=$[ end - start ]
		sql="select case when max(trandate)=decimal(current date -1 day) then 'Y' else 'N' end from administrator.hn_acctinfo where trandate < decimal(current date)"
		isSuccess=$(db2 -x "$sql")
	        if [ $isSuccess = "Y" ]; then
		    checkState acctinfo 4
	    	else
		    checkState acctinfo 5
		    exit
		fi
	else
		checkState acctinfo 3
		getTime=$[getTime+1]
		myDays=$[myDays+myDays]
		if [ $getTime -ge 3 ]; then
			break
		fi
		sleep 600
	fi
done

#断开连接
$ph/db2 connect reset

#关闭跟踪
set +x
