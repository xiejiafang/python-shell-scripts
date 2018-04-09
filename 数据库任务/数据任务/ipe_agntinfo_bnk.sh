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
	        #清空表
        	db2 "alter table administrator.hn_ipe_agntinfo_bnk activate not logged initially with empty table"
		db2 "insert into administrator.hn_ipe_agntinfo_bnk
		select 
		case when trim(a.branchtype) like '%续%' then 'TZ' else 'SQ' end as series,
		trim(a.agntbr),
		substr(a.agntnum,1,8),
		substr(a.agntname,1,40),
		INSIDEFLAG,
		a.agtype,
		value(int(trim(a.dteapp)),0),
		value(int(trim(a.end)),0),
		value(trim(a.teamhname),'0'),
		value(trim(a.teamh),'0'),
		value(trim(a.parthname),'0'),
		value(trim(a.parth),'0'),
		value(trim(a.receiptno),'0'),
		trim(secuityno) as id_num,
		0,
		'',
		case when a.teamnum is null then a.partnum else a.teamnum end,
		case when a.teamname is null then a.partname else a.teamname end,
		a.partnum,
		a.partname
		from administrator.ipe_agntinfo_bnk a left join administrator.IPE_AGNTINFO_CMS b on a.agntnum=b.agntnum
		where a.agntnum<>'0803063A' "
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


#下载数据
isSuccess="N"
getTime=0
while [ $isSuccess != "Y" ]
do
	#判断是否一致
	if [ $? = 0 ]; then
		isSuccess="Y"
		#更新FIC
		db2 "update administrator.hn_ipe_agntinfo_bnk set series='FIC' where (teamhname like '%销售三部%' or parthname like '%销售三部%') and branch='10'"
		#20140514张静申请拓展FIC人员划分
		db2 "update administrator.hn_ipe_agntinfo_bnk set teamh=parth where teamh=''"
        	db2 "update administrator.hn_ipe_agntinfo_bnk set series='TZF' where teamh like '%089A0037%'"
        	db2 "update administrator.hn_ipe_agntinfo_bnk set series='FIC' where agntnum in('08A02289','08A02590')"
		#20170101位岚岚申请划分人员
        	db2 "update administrator.hn_ipe_agntinfo_bnk set series='FIC' where agntnum in('08A03167','08A02212','08A03167','08A02289','08A03084','08A03745','08A03921')"
		#20170912位岚岚申请划分人员
        	db2 "update administrator.hn_ipe_agntinfo_bnk set series='FIC' where agntnum in('08A02289','08A03084','08A03168','08A03420')"
		#20150512添加考勤ID
		db2 "update administrator.hn_ipe_agntinfo_bnk a set (zknet_userid,badgenumber)=(select userid,badgenumber from administrator.hn_userinfo b where a.receiptno=b.ssn fetch first 1 rows only)  "

		#分类人员
		db2 "alter table administrator.hn_ipe_agntinfo_bnk_all activate not logged initially with empty table "
		db2 "insert into administrator.hn_ipe_agntinfo_bnk_all select * from administrator.hn_ipe_agntinfo_bnk where agt_lvl_ID1 in('2','3')"
		#删除B类人员
		db2 "delete from administrator.hn_ipe_agntinfo_bnk where AGT_LVL_ID1='2'"
		#记录结束时间
        	end=`date +%s`
        	#计算时间差
	        run_time=$[ end - start ]
		checkState ipe_agntinfo_bnk 4
	else
		checkState ipe_agntinfo_bnk 3
                getTime=$[getTime+1]
                if [ $getTime -ge 3 ]; then
                        break
                fi
		sleep 600
	fi
done

#断开连接
$ph/db2 connect reset

#关闭跟踪
set +x
