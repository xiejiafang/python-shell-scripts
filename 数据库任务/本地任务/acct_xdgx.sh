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
                	where date(info_time)=current date and message='数据正常!' and tablename in('agntinfo','xdgx')"
	db2 "select * from ($sql)a" |sed -n '4p'|awk '{print $1}' >acct_xdgx
	while read acct_xdgx
	do
        	isSuccess=$acct_xdgx
	done <acct_xdgx
	rm acct_xdgx
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
	#设置加工天数
	myDays=200

	#加工数据
	db2 "update administrator.hn_acctinfo a
        set acctamt_std_a=(select case when count(b.chdrnum) >1 then sum(xdgx)/count(b.chdrnum) when count(b.chdrnum)=1 then
 	sum(xdgx) else 0 end from administrator.hn_xdgx b 
                                where a.chdrnum=b.chdrnum and a.acctamt=b.txn_amt 
                                and a.trandate=b.trandate and b.xdgx<>0)
                where a.trandate between decimal(current date - $myDays days) and decimal(current date) "
	db2 "update administrator.hn_acctinfo a
        set fyc=acctamt*(select rate from administrator.hn_fyc_rate b where a.inss=b.crtable 
                                                                        and a.freq=b.freq
                                                                        and a.period=b.period)
                where a.trandate between decimal(current date - $myDays days) and decimal(current date) "
	#判断是否成功
	if [ $? = 0 ]; then
		isSuccess="Y"
		#记录结束时间
        	end=`date +%s`
        	#计算时间差
	        run_time=$[ end - start ]
		checkState acct_xdgx 4
		db2 "update administrator.hn_ipe_hpad a set acctamt_std=(
		        select sum(acctamt_std)
		                from administrator.hn_acctinfo b
               			         where a.chdrnum=b.chdrnum)
		                where hprrcvdt between decimal(current date - 5 days) and decimal(current date)"
	else
		checkState acct_xdgx 3
		sleep 10
	fi
done

#断开连接
$ph/db2 connect reset

#关闭跟踪
set +x
