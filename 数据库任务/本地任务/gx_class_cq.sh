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
	db2 "select * from ($sql)a" |sed -n '4p'|awk '{print $1}' >gx_class_cq
	while read gx_class_cq
	do
        	isSuccess=$gx_class_cq
	done <gx_class_cq
	rm gx_class_cq
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
	db2 "delete from administrator.hn_gx_class_cq --activate not logged initially with empty table"
	db2 "delete from administrator.gx_class_cq_agnt --activate not logged initially with empty table"
	db2 "
	INSERT INTO administrator.gx_class_cq_agnt            
	SELECT
	branch,
	aracde,
	a.agntnum,
	checkdate,
	badgenumber,
	date_date,
	cq,
	cx,
	pmcq,
	pmcx
	FROM 
	(select
	a.agntnum,
	badgenumber,
	date(checktime) AS checkdate,
	min(case when time(checktime) between '7:00:00' and '9:30:00' then time(checktime) end) as cq,
	min(case when time(checktime) between '10:30:00' and '12:30:00' then time(checktime) end) as cx,
	min(case when time(checktime) between '15:00:00' and '16:00:00' then time(checktime) end) as pmcq,
	min(case when time(checktime) between '18:00:00' and '19:00:00' then time(checktime) end) as pmcx
	from administrator.hn_checkinout a 
	where checkdate between decimal(current date - 6 months) and decimal(current date)
	AND (agntnum in(SELECT agntnum FROM administrator.hn_agntinfo b WHERE b.dteapp >= decimal(current date - 6 months))
	or a.agntnum is null or a.agntnum like 'XN%')
	group by a.agntnum,badgenumber,date(checktime)) a,
	(SELECT
	f.getbranchname(branch) as branch,
	administrator.getaracdename(aracde) as aracde,
	agntnum,
	date_date
	FROM administrator.hn_agntinfo a,administrator.hn_date_list c 
	WHERE dteapp >= decimal(current date - 5 months)
	AND dteapp=c.date_int) b
	WHERE a.agntnum=b.agntnum       
	"
   	db2 "
	insert into administrator.hn_gx_class_cq
	with cq as(
	    select
	      *
	      from administrator.gx_class_cq_agnt),
	rl as(        
	select 
	  userid_zknet,
	  sign_no,
	  name,
	  class_name,
	  startdate,
	  enddate
	  from administrator.hn_class_person_info a,administrator.hn_date_list b
	    where enddate = date_date
	    and date_int between decimal(current date - 6 months) and decimal(current date)),
	list as(    
	select 
	  substr(class_name,1,100) as class_name,
	  name,
	  sign_no,
	  startdate,
	  enddate,
	  branch,
	  aracde,
	  agntnum,
	  date_date,
	  badgenumber,
	  checkdate,
	  cq,
	  cx,
	  pm_cq,
	  pm_cx
	  from rl a right join cq b on a.userid_zknet=b.badgenumber   
	      where userid_zknet is not null
	      and (checkdate <= date_date + 90 days or date_date is null))
	select 
	  *
	  from list     
	"
	#判断是否成功
	if [ $? = 0 ]; then

	  	db2 "update administrator.hn_gx_class_cq set name='杨云杰' where name like '%杨云杰%'"
		isSuccess="Y"
		#记录结束时间
        	end=`date +%s`
        	#计算时间差
	        run_time=$[ end - start ]
		checkState gx_class_cq 4
	else
		isSuccess="Y"
		checkState gx_class_cq 5
		sleep 10
	fi
done

#断开连接
$ph/db2 connect reset

#关闭跟踪
set +x
