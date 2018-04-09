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
	#定义游标
	db2 "declare c1 cursor for select a.AGNTBR,a.ARACDE,a.CNTCDE,a.ARACDE||a.CNTCDE as fgcommtabl,b.DESCITEM ,
	b.SHORTDESC,b.LONGDESC,a.EFFDATE,a.REPORTAG,a.MLAGTTYP,a.ZDIRCNT,a.VALIDFLAG,trim(c.agntname)
		from administrator.ZCNTPF a left join
	(select DESCITEM ,SHORTDESC,LONGDESC from administrator.descpf where descpfx='IT' and desctabl='TYX82'
	and LANGUAGE ='S' and desccoy='D' ) b
		on a.AGNTBR||a.ARACDE||a.CNTCDE=b.DESCITEM left join administrator.ipe_agntinfo c on a.reportag=c.agntnum"
	if [ $? != 0 ]; then
		checkState zcntinfo 2	
		sleep 60
		continue
	fi
	#防止"3"错误
	db2 "load from test.txt of del terminate into administrator.hn_zcntinfo"
	#清空表
	db2 "alter table administrator.hn_zcntinfo activate not logged initially with empty table"
	#加工数据
	db2 "load from c1 of cursor insert into administrator.hn_zcntinfo"
	#核对数据
	sql="select case when a.row=b.row then 'Y' else 'N' end from (select count(*) as row from administrator.zcntpf) a,(select count(*) as row from administrator.hn_zcntinfo) b"
	db2 "select * from ($sql)a" |sed -n '4p'|awk '{print $1}' >zcntinfo
	while read zcntinfo
	do
        	isSuccess=$zcntinfo
	done <zcntinfo
	rm zcntinfo
	#判断是否一致
	if [ $isSuccess = "Y" ]; then
		#记录结束时间
        	end=`date +%s`
        	#计算时间差
	        run_time=$[ end - start ]
		checkState zcntinfo 4
	else
		checkState zcntinfo 3
                getTime=$[getTime+1]
                if [ $getTime -ge 3 ]; then
                        break
                fi
		sleep 60
	fi
done

#断开连接
$ph/db2 connect reset

#关闭跟踪
set +x
