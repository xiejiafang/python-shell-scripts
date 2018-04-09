#!/bin/sh
db2 connect to hnii;

while read line
do
	tablename=`echo $line |awk '{print $1}'`;
	echo $tablename;
	db2 "create nickname administrator.$tablename for tk_info.zhengzh.$tablename";
done <$1
