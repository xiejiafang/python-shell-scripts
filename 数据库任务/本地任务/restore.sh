#!/bin/sh
#定义命令路径
ph='/home/db2inst/sqllib/bin'

#引入用户环境变量
source /home/db2inst/.bash_profile

#开始计时
start=`date +%s`

#连接数据库
$ph/db2 "connect to hnii"  

#恢复文件
file="/home/db2inst/backupAndRestore/`date +%F`"
echo "准备从以下备份中恢复：$file"

#杀死所有的应用程序
echo "杀掉所有应用："
$ph/db2 "force applications all"

#恢复
echo "恢复数据库："
$ph/db2 "restore database hnii from $file "

#结束记时
end=`date +%s`

#计算用时
run_time=$[ end - start ]

echo "共计用时：$run_time秒";
