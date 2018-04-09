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

#设定数组
array=(ipe_rtrn_rt ipe_acct_rt ipe_hpad_rt)

#数组循环
for data in ${array[@]}
do
	#开始计时
	start=`date +%s`
	#下载数据
	isSuccess="N"
	#更改字段
	if [ $data = "ipe_hpad_rt" ]; then
		col="hprrcvdt"
		select_col="COMPANY,CHDRNUM,ACCTAMT,ACCTAMT_STD,ACCTAMT_STD_A,CAMPAIGN,BATC_TYPE,BATC_TYPE_C,AGNTNUM,AGNTNAME,AGTYPE,TEAMNUM,TEAMNAME,TEAMTYPE,PARTNUM,PARTNAME,PARTTYPE,FGCOMMTABL,ARACDE,BRANCH,HPRRCVDT,CNTTYPE,ARACDE_XUQI,FGCOMMTABL_XUQI,datime"
	else
		col="trandate"
		select_col="*"
	fi
	
	while [ $isSuccess = "N" ]
	do
		#删除原有数据
		db2 "delete from administrator.hn_${data} where $col = decimal(current date)"
		#加工新数据
		db2 "insert into administrator.hn_${data} 
			select $select_col from administrator.${data} where $col = decimal(current date) and acctamt<50000000"
		#判断是否成功
		if [ $? = 0 ] ; then
			isSuccess="Y"
			#记录结束时间
       	 		end=`date +%s`
       		 	#计算时间差
	      		run_time=$[ end - start ]
			#checkState ${data} 4
		else
			isSuccess="Y"
			#记录结束时间
			#记录结束时间
        		end=`date +%s`
       	 		#计算时间差
	       		run_time=$[ end - start ]
			checkState ${data} 5
			sleep 10
		fi
	done
done
db2 "update administrator.hn_ipe_acct_rt a set (branch,aracde)=(select branch,aracde from administrator.hn_agntinfo b where a.agntnum=b.agntnum) 
          where trandate=decimal(current date)"
db2 "update administrator.hn_ipe_rtrn_rt a set (branch,aracde)=(select branch,aracde from administrator.hn_agntinfo b where a.agntnum=b.agntnum) 
          where trandate=decimal(current date)"
db2 "update administrator.hn_ipe_hpad_rt a set (branch,aracde)=(select branch,aracde from administrator.hn_agntinfo b where a.agntnum=b.agntnum) 
          where hprrcvdt=decimal(current date)"
db2 "update administrator.hn_ipe_acct_rt set branch='80' where (branch='I0' or branch='' or branch is null) and trandate >=20180101  with ur"
db2 "update administrator.hn_ipe_rtrn_rt set branch='80' where (branch='I0' or branch='' or branch is null) and trandate >=20180101  with ur"
db2 "update administrator.hn_ipe_hpad_rt set branch='80' where (branch='I0' or branch='' or branch is null) and hprrcvdt >=20180101  with ur"
			
#20140812添加：将实时承保业绩插入到acctinfo中
db2 "delete from administrator.hn_acctinfo where trandate = decimal(current date)"
db2 "insert into administrator.hn_acctinfo
SELECT
    CHDRNUM, INS, FREQ, INSS, value(PERIOD,0), ACCTAMT, ACCTAMT_STD, SUMINS, TRANDATE, BATCTRCDE,
    BATC_TYPE, BATC_TYPE_C, LT_IND, HC_IND, AGNTNUM, AGNTNAME, AGTYPE, TEAMNUM, TEAMNAME,
    TEAMTYPE, PARTNUM, PARTNAME, PARTTYPE, ARACDE, BRANCH, SACSCODE, SACSTYP, CNTTYPE,
    HPRRCVDT, 0, VAL_RATE, ACCTAMT_STD_A, SRCEBUS, ''
FROM
    ADMINISTRATOR.HN_IPE_ACCT_RT
       WHERE trandate = decimal(current date)	"
db2 "UPDATE administrator.hn_acctinfo a SET fyc=acctamt_std* ( SELECT rate FROM administrator.hn_fyc_rate b WHERE a.inss=b.crtable AND a.freq=b.freq AND a.period=b.period) WHERE a.trandate = DECIMAL(CURRENT DATE)"

#20141225添加：将实时业绩插入到hn_ipe_hpad中
db2 "delete from administrator.hn_ipe_hpad where HPRRCVDT = decimal(current date)"
db2 "insert into administrator.hn_ipe_hpad
select 
        COMPANY,CHDRNUM,ACCTAMT,ACCTAMT_STD,ACCTAMT_STD_A,CAMPAIGN,BATC_TYPE,BATC_TYPE_C,
        AGNTNUM,AGNTNAME,AGTYPE,TEAMNUM,TEAMNAME,TEAMTYPE,PARTNUM,PARTNAME,PARTTYPE,FGCOMMTABL,ARACDE,
        BRANCH,HPRRCVDT,CNTTYPE,ARACDE_XUQI,FGCOMMTABL_XUQI
FROM
    ADMINISTRATOR.HN_IPE_HPAD_RT
       WHERE HPRRCVDT = decimal(current date)   "

#20141225添加：将实时业绩插入到hn_rtrninfo中
db2 "delete from administrator.hn_rtrninfo where trandate = decimal(current date)"
db2 "insert into administrator.hn_rtrninfo
select 
        CHDRNUM,ACCTAMT,ACCTAMT_STD,CAMPAIGN,trandate,BATC_TYPE,BATC_TYPE_C,
        AGNTNUM,AGNTNAME,AGTYPE,TEAMNUM,TEAMNAME,TEAMTYPE,PARTNUM,PARTNAME,PARTTYPE,ARACDE,
        BRANCH,HPRRCVDT,CNTTYPE,'',batctrcde
FROM
    ADMINISTRATOR.HN_IPE_rtrn_RT
       WHERE trandate = decimal(current date)   "

#调用KPI过程
#db2 "call p.get_gxkpi(current date +1 days)"


#20161009添加当日入司人力
db2 "delete from administrator.hn_agntinfo where dteapp=decimal(current date)"
db2 "
	insert into administrator.hn_agntinfo
	select REPORTAG,a.AGNTNUM,a.AGNTNAME,a.CLNTNUM,a.DTEAPP,a.DTETRM,a.AGTYPE,a.AGTYPEDATE,a.TASADATE,
	a.TEAMNUM, a.TEAMname,left(trim(a.teamtype),2),a.partNUM,a.partname,left(trim(a.parttype),2),
	(case when a.agtype='RC' and aracde_xuqi<>'' then a.aracde_xuqi
	when a.agtype='RC' and aracde_xuqi='' then a.aracde else a.ARACDE end )as aracde,
	a.BRANCH,a.ZRECRUIT,a.ZRECRUITNAME,a.CONTPERS,a.EXCL_AGMT,
	case when aGtype='RC' THEN a.fgcommtabl_XUQI ELSE a.fgcommtabl END ,
	a.secuityno,a.cltsex,a.cltdob,rtrim(agntaddr01)||rtrim(agntaddr02)||rtrim(agntaddr03),
	substr(agntphone01,1,16),substr(agntphone02,1,16),a.zedulvl,ridesc,value(xqtype,'-')
	from ADMINISTRATOR.ipe_agntinfo a
	where dteapp=decimal(current date)
"
#断开连接
$ph/db2 connect reset

#关闭跟踪
set +x
