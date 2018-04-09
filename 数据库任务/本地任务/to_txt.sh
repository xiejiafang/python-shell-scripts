#!/bin/sh

#引入用户环境变量
source /home/db2inst/.bash_profile

##开始计时
start=`date +%s`

#连接本地库
filename="report_list"
sql="
        export to $filename.txt of del
	SELECT id,team_id,name_zh,name,priority,date_to,created_on,ipaddr from db_33.$filename
	"
db2 -x "$sql" 
scp $filename.txt it@10.19.19.108:/home/it

filename="report_team"
sql="
        export to $filename.txt of del
	SELECT * from db_33.$filename
	"
db2 -x "$sql" 

#断开连接
db2 connect reset

