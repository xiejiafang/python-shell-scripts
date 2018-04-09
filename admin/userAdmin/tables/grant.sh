#!/bin/sh

#连接数据库

#列出所有表，并授权
source ./getTableList.sh 0
read -p "被授权的用户名：" answer
var=$answer

while read line
do
	echo  "grant select on administrator.$line to user $var";
	db2 "grant select on administrator.$line to user $var";
done < tableList`date +%F`
exit
