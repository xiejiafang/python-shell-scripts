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
			message="加工失败，10秒后重新运行!"
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
isSuccess="Y"
while [ $isSuccess = "N" ]
do
	sql="select case when count(*)>=1 then 'Y' else 'N' end
        	from info.taskmessage
                	where date(info_time)=current date and message='数据正常!' and tablename in('agntinfo','agency_group','basdata','acctinfo','chdrinfo','zcdrpf','ybagent')"
	db2 "select * from ($sql)a" |sed -n '4p'|awk '{print $1}' >agnt_his
	while read agnt_his
	do
        	isSuccess=$agnt_his
	done <agnt_his
	rm agnt_his
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
	db2 "delete from administrator.hn_agnt_his_simpleness where mon=int(decimal(current date -1 day)/100)"
	db2 "insert into administrator.hn_agnt_his_simpleness
		select 
        		agntnum,
        		agtype,
        		agtypedate,
     		   	teamnum,
        		partnum,
        		aracde,
        		branch,
        		int(decimal(current date -1 day)/100),
                        '',
                        xqtype 
        	from administrator.hn_agntinfo a"
	#判断是否成功
	if [ $? = 0 ]; then
		isSuccess="Y"
		#更新状态
                db2 "update administrator.hn_agnt_his_simpleness a 
                set status=case when (select int(dtetrm/100) from administrator.hn_agntinfo b where a.agntnum=b.agntnum)>mon then 'Y' else 'N' end where mon=int(decimal(current date -1 day)/100)"
		#记录结束时间
        	end=`date +%s`
        	#计算时间差
	        run_time=$[ end - start ]
		checkState agnt_his 4
	else
		checkState agnt_his 5
		sleep 10
	fi
done

#断开连接
$ph/db2 connect reset

#关闭跟踪
set +x
