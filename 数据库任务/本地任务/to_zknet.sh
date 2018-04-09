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
	sql="select case when count(*)>=1 then 'Y' else 'N' end
        	from info.taskmessage
                	where date(info_time)=current date and message='数据正常!' and tablename in('class_person_info')"
	db2 "select * from ($sql)a" |sed -n '4p'|awk '{print $1}' >to_zknet
	while read to_zknet
	do
        	isSuccess=$to_zknet
	done <to_zknet
	rm to_zknet
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
	db2 "insert into hnii235.hn_xinren_zknet
	select 
	        substr(branch,1,2) as branch,
	        substr(branch,3,3) as aracde,
	        substr(name,1,9),
	        trim('XN0'||char(substr((select max(agntnum) from hnii235.hn_xinren_zknet),4,5)+row_number()over())) as maxnum,
	        sign_no,
	        'XN' as agtype,
	        '9' as temp,
	        decimal(startdate) as c_date,
	        0 as dtetrm
	                from administrator.hn_class_person_info
	                        where sign_no not in(
	                                select secuityno from hnii235.hn_xinren_zknet)
	                        and startdate between current date -1 month and current date
	                        and substr(branch,1,2) in(select branch from administrator.hn_branch)"
	#判断是否成功
	if [ $? -eq 0 ]; then
		isSuccess="Y"
		#记录结束时间
        	end=`date +%s`
        	#计算时间差
	        run_time=$[ end - start ]
		checkState to_zknet 4
	else
		checkState to_zknet 3
		sleep 10
	fi
done

#断开连接
$ph/db2 connect reset

#关闭跟踪
set +x
