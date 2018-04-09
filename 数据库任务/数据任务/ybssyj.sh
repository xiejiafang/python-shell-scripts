#!/bin/sh
#定义命令路径
ph='/home/db2inst/sqllib/bin'

#引入用户环境变量
source /home/db2inst/.bash_profile


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
			message="加工失败，10秒后重试!"
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
	$ph/db2 "connect to tk_90_1 user zhengzh using zhengzh33y"  >/dev/null
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
$ph/db2 connect to hnii


#开始计时
start=`date +%s`
#下载数据
isSuccess="Y"
while [ $isSuccess = "N" ]
do
	#加工新数据
	db2 "call p.get_ybssyj "
	#判断是否成功
	if [ $? = 0 ]; then
		isSuccess="Y"
		#记录结束时间
       		end=`date +%s`
       		#计算时间差
	      	run_time=$[ end - start ]
		#checkState ybssyj 4
		#db2 "delete from hnii29.hn_ybyj where trandate=decimal(current date)"
		#db2 "insert into hnii29.hn_ybyj select * from administrator.hn_ybyj where trandate=decimal(current date)"
	else
		#记录结束时间
		isSuccess="Y"
        	end=`date +%s`
       	 	#计算时间差
	       	run_time=$[ end - start ]
		checkState ybssyj 5
		sleep 10
	fi
done

#20140812添加实时业绩：
db2 "delete from administrator.hn_ipe_acct_bnk where trandate=decimal(current date)"
db2 "insert into administrator.hn_ipe_acct_bnk
	select
	a.prod_cat,
	case when b.series is null then case when chnl_id=2 then 'SQ' else 'TZ' end else b.series end,
	value(b.branch,substr(ind_code,2,2)),
	0,
	value(bk_lgcy_nbr,''),
	plcy_lgcy_nbr,
	trandate,
	value(plcy_type_lgcy_nbr,''),
	txn_amt,
	st_txn_amt,
	st_txn_amt,
	value(vl_txn_amt,0),
	'',
	0,
	'',
	agt_lgcy_nbr,
	b.agntname,
	b.teamnum,
	b.teamname,
	''
	from administrator.f_rt_evt_bnk a left join administrator.hn_ipe_agntinfo_bnk b on a.agt_lgcy_nbr=b.agntnum
	where company='D' 
	and trandate =decimal(current date)"

#断开连接
$ph/db2 connect reset

#关闭跟踪
set +x
