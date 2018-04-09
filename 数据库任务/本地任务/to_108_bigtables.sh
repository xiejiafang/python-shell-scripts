#!/bin/sh

#引入用户环境变量
source /home/db2inst/.bash_profile

##开始计时
start=`date +%s`

#连接本地库


#checkdate
filename="kqinfo"
sql="
	export to $filename.txt of del
	SELECT
	a.agntnum,
	BADGENUMBER,
	checkdate,
	MIN(checktime) AS checktime_min,
	MAX(checktime) AS checktime_max,
	CASE
	WHEN hour(MAX(checktime))*3600+minute(MAX(checktime))*60+second(MAX(checktime)) -hour(MIN
	(checktime))*3600-minute(MIN(checktime))*60-second(MIN(checktime))>=1800
	THEN '有效'
	ELSE '无效'
	END AS flag,
	b.agtype
	FROM
	administrator.hn_checkinout a left join administrator.hn_agntinfo b on a.agntnum=b.agntnum
	WHERE
	checkdate between int(decimal(current date)/100)*100 and decimal(current date)
	and b.xqtype not like '%CA%' AND b.xqtype not like '%CS%'
	--checkdate = (select max(checkdate) from administrator.hn_checkinout)
	GROUP BY
	a.agntnum,
	checkdate,
	BADGENUMBER,
	agtype
	"
db2 -x "$sql" 
scp $filename.txt it@10.19.19.108:/home/it
rm $filename.txt


#调用远程脚本
ssh it@10.19.19.108 "/home/it/bigfiles.sh"

#断开连接
db2 connect reset

