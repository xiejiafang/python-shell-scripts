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

#下载数据
isSuccess="N"
getTime=0
while [ $isSuccess != "Y" ]
do
	#定义游标
	db2 "declare c1 cursor for 
	select REPORTAG,a.AGNTNUM,a.AGNTNAME,a.CLNTNUM,a.DTEAPP,a.DTETRM,a.AGTYPE,a.AGTYPEDATE,a.TASADATE,
	a.TEAMNUM, a.TEAMname,left(trim(a.teamtype),2),a.partNUM,a.partname,left(trim(a.parttype),2),
	(case when a.agtype='RC' and aracde_xuqi<>'' then a.aracde_xuqi
	when a.agtype='RC' and aracde_xuqi='' then a.aracde else a.ARACDE end )as aracde,
	a.BRANCH,a.ZRECRUIT,a.ZRECRUITNAME,a.CONTPERS,a.EXCL_AGMT,
	case when aGtype='RC' THEN a.fgcommtabl_XUQI ELSE a.fgcommtabl END ,
	a.secuityno,a.cltsex,a.cltdob,'',--rtrim(agntaddr01)||rtrim(agntaddr02)||rtrim(agntaddr03),
	substr(agntphone01,1,6),substr(agntphone02,1,6),a.zedulvl,ridesc,value(xqtype,'RC')
        	from ADMINISTRATOR.ipe_agntinfo a"
	if [ $? != 0 ]; then
		checkState agntinfo 2	
		sleep 10
		continue
	fi
	#防止"3"错误
	db2 "load from test.txt of del terminate into administrator.hn_agntinfo"
	#清空表
	db2 "alter table administrator.hn_agntinfo activate not logged initially with empty table"
	#加工数据
	db2 "load from c1 of cursor insert into administrator.hn_agntinfo"
	#更新数据
	db2 "update administrator.hn_agntinfo set agtype ='CR' where agntnum like 'YD%'"
	#核对数据
	sql="select case when a.row=b.row then 'Y' else 'N' end from (select count(*) as row from administrator.ipe_agntinfo) a,(select count(*) as row from administrator.hn_agntinfo) b"
	db2 "select * from ($sql)a" |sed -n '4p'|awk '{print $1}' >agntinfo
	while read agntinfo
	do
        	isSuccess=$agntinfo
	done <agntinfo
	rm agntinfo
	#判断是否一致
	if [ $isSuccess = "Y" ]; then
		$ph/db2 "update administrator.hn_agntinfo set branch='80' where branch='I0'"
		$ph/db2 "update administrator.hn_agntinfo set aracde='807' where aracde='I01'"
		$ph/db2 "update administrator.hn_agntinfo set aracde='8AA' where aracde='IAA'"
		$ph/db2 "delete from administrator.hn_agntinfo where agtype in ('PA','AC','DL')"
		#记录结束时间
        	end=`date +%s`
        	#计算时间差
	        run_time=$[ end - start ]
		checkState agntinfo 4
		source /home/db2inst/数据库任务/本地任务/agnt_his.sh
	else
		checkState agntinfo 3
		getTime=$[getTime+1]
		if [ $getTime -eq 3 ];then
			break
		fi
		sleep 120
	fi
done

#断开连接
$ph/db2 connect reset

#关闭跟踪
set +x
