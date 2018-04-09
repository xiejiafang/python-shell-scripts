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
			message="加工失败，10秒后重新加工!"
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
	sql="select case when count(*)>=3 then 'Y' else 'N' end
        	from info.taskmessage
                	where date(info_time)=current date and message='数据正常!' and tablename in('acctinfo','agntinfo','zcllinfo')"
	db2 "select * from ($sql)a" |sed -n '4p'|awk '{print $1}' >gxkpi
	while read gxkpi
	do
        	isSuccess=$gxkpi
	done <gxkpi
	rm gxkpi
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
	#加工数据
	db2 "call p.run_kpi(current date,current date)"
	#20140620添加
	db2 "delete from hnii29.hn_gxkpi_aracde where  kpi_name='2WP'"
	db2 "insert into hnii29.hn_gxkpi_aracde select * from administrator.hn_gxkpi_aracde where  kpi_name='2WP'"
	db2 "delete from hnii29.hn_gxkpi where  kpi_name='2WP'"
	db2 "insert into hnii29.hn_gxkpi select * from administrator.hn_gxkpi where  kpi_name='2WP'"
	state=$?
	db2 "call p.get_report_branch()"
	state=$[state+$?]
	db2 "call p.get_index_branch()"
	state=$[state+$?]
	db2 "call p.get_report_branch_qp()"
	state=$[state+$?]
	db2 "call p.get_report_station()"
	state=$[state+$?]
	db2 "call p.get_report_cqbb()"
	state=$[state+$?]
	#判断是否成功
	if [ $state = 0 ]; then
		isSuccess="Y"
		#记录结束时间
        	end=`date +%s`
        	#计算时间差
	        run_time=$[ end - start ]
		checkState gxkpi 4
	else
		checkState gxkpi 5
		if [ `date +%H` -ge 9 ];then
			break;
		fi
		sleep 600
	fi
done

#断开连接
$ph/db2 connect reset

#关闭跟踪
set +x
