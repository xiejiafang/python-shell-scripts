#!/bin/sh

#引入环境变量
source /home/db2inst/.bash_profile

#记录开始时间
start=`date +%s`

#打开跟踪功能
set -x;

#判断数据库是否开放
#isOpen="false"
#while [ $isOpen == "false" ]
#do
#        $ph/db2 connect to hnii
#        $ph/db2 "select * from hnii0.$var fetch first 1 rows only" >/dev/null
#        if [ $? == 0 ];then
#                isOpen="true"
#        else
#                echo "查询表失败，10秒钟后重试。"
#                sleep 10
#        fi
#done

#连接数据库
db2 connect to hnii;
db2 "set current schema administrator"

#定义游标
db2 "declare c1 cursor for select chdrnum,lifcnum,datime,life from administrator.lifepf" 

#防止"3"错误
db2 "load from test.txt of del terminate into administrator.hn_lifepf"

#清空表
db2 "alter table administrator.hn_lifepf activate not logged initially with empty table"

#从游标中插入数据
db2 "load from c1 of cursor insert into administrator.hn_lifepf  "|grep Number

#启用压缩 
db2 "alter table hn_lifepf compress yes"
	
#重组表
db2 "reorg table hn_lifepf"

#记录结束时间
end=`date +%s`

#计算时间差
run_time=$[ end - start ]

set +x;

db2 connect reset;
