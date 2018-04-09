#!/bin/sh

#连接数据库
db2 connect to hnii

#列出所有表，并授权
source ./getFunctions.sh
read -p "被授权的用户名：" answer
var=$answer

while read line
do
	echo  "grant execute on function administrator.$line to user $var";
	db2 "grant execute on function administrator.$line to user $var";
	db2 "grant execute on function f.$line to user $var";
done < content`date +%F`
exit
