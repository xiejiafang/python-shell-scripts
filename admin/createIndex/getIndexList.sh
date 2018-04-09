#!/bin/sh
db2 connect to hnii
db2 "set current schema administrator"
#获取远程库的表
db2 "select indname,tabname,replace(substr(colnames,2),'+',',')
	from hnii0.syscat_indexes where tabschema = 'ADMINISTRATOR'" >table
#删除空白行
sed '/^[\t]*$/d' table >tables
#删除前2行
sed '1,2d' tables >table
#删除最后一行
sed '$d' table >tables
#更改文件名
mv tables indexList`date +%F`
rm table
