#!/bin/sh

#引入用户环境变量
source /home/db2inst/.bash_profile

##开始计时
start=`date +%s`

#连接本地库
db2 connect to hnii
filename="cbbf"
sql="
	export to cbbf.txt of del
	SELECT
	aracde,
	sum(acctamt_std)/10000
	FROM administrator.hn_acctinfo
	WHERE agtype in(select agtype from administrator.hn_agtype)
	AND trandate BETWEEN 20180101 AND decimal(current date)
	GROUP BY aracde
	"
db2 -x "$sql" 
scp $filename.txt it@10.19.19.38:/home/it
rm $filename.txt
ssh it@10.19.19.38 "/home/it/load_data.sh"

#断开连接
db2 connect reset

