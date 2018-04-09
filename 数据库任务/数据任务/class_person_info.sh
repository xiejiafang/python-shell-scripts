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

#下载数据
isSuccess="N"
getTime=0
while [ $isSuccess != "Y" ]
do
	#更新考勤信息
	db2 "update administrator.hn_userinfo a set email=(select secuityno from administrator.hn_agntinfo b where a.ssn=b.agntnum)
        where exists(select * from administrator.hn_agntinfo c where c.agntnum=a.ssn)"
	#清空权限
	db2 "revoke select on administrator.hn_class_person_info from report"
	#清空表
	db2 "delete from administrator.hn_class_person_info"
	#加工数据
	db2 "insert into administrator.hn_class_person_info 
	select 
        case when length(trim(a.user_id))=9 then a.id_num else a.user_id end,
        substr(a.name,1,9),
        b.class_name,
        case when length(aracde)=3 then substr(substr(b.dep_id,1,3)||aracde,2,5) else substr(aracde,2,5) end,
        b.fact_startdate,
        b.fact_enddate,
        a.certificate_flag,
	0,
	a.CERTIFICATE_NUM,
	a.identity
        from administrator.class_student a,administrator.classinfo b 
                where a.class_id=b.id
                and b.com_id='D' and b.class_name like '%新人岗前%'
		--and a.certificate_flag='1'
                and b.startdate between current date - 6 months and current date "
	if [ $? != 0 ]; then
		checkState class_person_info 2	
		sleep 10
		continue
	fi
	#判断是否一致
	if [ $? = 0 ]; then
		db2 "update administrator.hn_class_person_info a set userid_zknet= (select c.badgenumber from administrator.hn_userinfo c where a.sign_no =c.ssn or  a.sign_no=c.email and title='AG' fetch first 1 rows only) "
		db2 "update administrator.hn_class_person_info a set branch=(select branch||aracde from administrator.hn_agntinfo b where a.sign_no=b.agntnum) where branch is null"
		db2 "update administrator.hn_class_person_info a set name=(select substr(trim(agntname),1,15) from administrator.hn_agntinfo b where a.sign_no=b.secuityno fetch first 1 rows only) where exists (select * from administrator.hn_agntinfo c where a.sign_no=c.secuityno) "
		db2 "update administrator.hn_class_person_info a set sign_no=(select trim(secuityno) from administrator.hn_agntinfo b where a.sign_no=b.agntnum) where length(sign_no)=8 "
		db2 "update administrator.hn_class_person_info set branch=replace(branch,'O','0') where substr(branch,1,2) not in(select branch from administrator.hn_branch) "
		db2 "update administrator.hn_class_person_info set branch=replace(branch,'I0','80') where substr(branch,1,2) not in(select branch from administrator.hn_branch) "
		db2 "update administrator.hn_class_person_info set branch=replace(branch,'801','8AA') where substr(branch,1,2) not in(select branch from administrator.hn_branch) "
		isSuccess="Y"
		#记录结束时间
        	end=`date +%s`
        	#计算时间差
	        run_time=$[ end - start ]
		checkState class_person_info 4
		db2 "grant select on administrator.hn_class_person_info to user report"
	else
		checkState class_person_info 3
                getTime=$[getTime+1]
                if [ $getTime -ge 3 ]; then
                        break
                fi
		sleep 120
	fi
done

#断开连接
$ph/db2 connect reset

#关闭跟踪
set +x
