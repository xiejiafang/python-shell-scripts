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
$ph/db2 connect to hnii

#判断依赖表是否加工完成
isSuccess="N"
while [ $isSuccess != "Y" ]
do
	sql="select case when count(*)>=1 then 'Y' else 'N' end
        	from info.taskmessage
                	where date(info_time)=current date and message='数据正常!' and tablename in('ipe_agntinfo_bnk')"
	db2 "select * from ($sql)a" |sed -n '4p'|awk '{print $1}' >ipe_acct_bnk
	while read ipe_acct_bnk
	do
        	isSuccess=$ipe_acct_bnk
	done <ipe_acct_bnk
	rm ipe_acct_bnk
	#判断是否一致
	if [ $isSuccess = "Y" ]; then
		echo "依赖表加工成功"
	else
		sleep 20
	fi
done

#更新数据
isSuccess="N"
getTime=0
while [ $isSuccess != "Y" ]
do
	result=`db2 -x "select  case when max(trandate)=decimal(current date -1 days) then 'YYY' else 'NNN' end as check_result from administrator.ipe_acct_bnk where trandate<decimal(current date)"`
	if [ $result == "NNN" ]; then
		sleep 600;
		continue;
	fi
			
	db2 "call p.get_hn_ipe_acct_bnk()"
	#记录结束时间
        end=`date +%s`
	#更新险种
	db2 "update administrator.hn_ipe_acct_bnk a set cnttype=(select cnttype from administrator.hn_chdrinfo b where a.chdrnum=b.chdrnum) where a.prod_cat='IND'"
	db2 "update administrator.hn_ipe_acct_bnk a set cnttype=(select kinds from administrator.hn_basdata b where a.chdrnum=b.bm_cert and app_flag='1' fetch first 1 rows only) where a.prod_cat='BNK'"
    
        db2 "update administrator.hn_ipe_acct_bnk a SET statcode = (SELECT statcode FROM administrator.IPE_CHDRINFO_BNK b WHERE a.chdrnum=b.chdrnum)
	    WHERE statcode IS NULL
	    AND trandate BETWEEN 20160101 AND decimal(current date)"
    
	db2 "update administrator.hn_ipe_acct_bnk a SET cnttype = (SELECT cnttype FROM administrator.IPE_CHDRINFO_BNK b WHERE a.chdrnum=b.chdrnum)
	    WHERE cnttype IS NULL
	    AND trandate BETWEEN 20160101 AND decimal(current date) "
        #计算时间差
	run_time=$[ end - start ]
	#判断是否成功
	if [ $? = 0 ]; then
		#db2 "delete from hnii29.hn_ipe_acct_bnk"
		#db2 "insert into hnii29.hn_ipe_acct_bnk select * from administrator.hn_ipe_acct_bnk where trandate > year(current date -2 year)*10000"
		isSuccess="Y"
		#记录结束时间
        	end=`date +%s`
        	#计算时间差
	        run_time=$[ end - start ]
		checkState ipe_acct_bnk 4
	else
		checkState ipe_acct_bnk 3
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
