#!/bin/sh

#引入用户环境变量
source /home/db2inst/.bash_profile


#连接本地库

isSuccess="N"
while [ $isSuccess = "N" ]
do
	sql="select count(*),current timestamp from administrator.hn_kpi_information where kpi_mon <201701 " 
	db2 -x "$sql" >>kpi_log
	sleep 600
done


#断开连接
db2 connect reset

