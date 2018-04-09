#!/bin/sh
#定义命令路径
ph='/home/db2inst/sqllib/bin'

#引入用户环境变量
source /home/db2inst/.bash_profile

#开启跟踪
#set -x

#连接本地库
isConnect='N'
while [ $isConnect = "N" ]
do
	$ph/db2 connect to hnii
	if [ $? = 0 ]; then
		isConnect="Y"
	else
		sleep 20
	fi
done

#检查表
time=0
isSuccess="N"
source /home/db2inst/数据库任务/本地任务/checkdata.sh
source /home/db2inst/数据库任务/本地任务/checkdata_jan.sh
while [ $isSuccess = "N" ]
do
	tableList=""
	db2 " with taskinfo as(
	select * 
        	from info.taskmessage a
                	where date(info_time) = current date 
                	and info_time =(select max(info_time) from info.taskmessage c
                                        where c.tablename=a.tablename))
	select a.tablename from info.checklist a left join taskinfo b on a.tablename=b.tablename 
        	where message <> '数据正常!' " |sed  '1,3d' |sed '/^[\t]*$/d'|sed '$d'|awk '{print $1}' >check
	while read check
	do
        	tableList="$tableList$check  "
	done <check
	rm check
	#判断是否一致
	if [ -z "$tableList" ]; then
		isSuccess="Y"
		tableList="[34数据库]总分数据同步成功！"
		$ph/db2 "insert into hnii0.hn_sms_msg(SMSFL,HANDSETNO,SMSINFO,STATCODE,USERID,DS_TIME)
		select 'datacheck',handphone,'$tableList','0','XIE',date(current timestamp)
			from administrator.sms_list where smsfl = 'CHECK'"
	else
		time=$[time+1]
		if [ $time -gt 3 ]; then
                        break
                else
			tableList="[34数据库]:$tableList加工失败,第$time次提醒!"
			$ph/db2 "insert into hnii0.hn_sms_msg(SMSFL,HANDSETNO,SMSINFO,STATCODE,USERID,DS_TIME)
			select 'datacheck',handphone,'$tableList','0','XIE',date(current timestamp)
				from administrator.sms_list where smsfl = 'CHECK'"
		fi
		sleep 3600
	fi
done

#断开连接
$ph/db2 connect reset

#关闭跟踪
