#!/bin/sh
db2 "set current schema administrator"
#SQL语句
sql="select funcname 
        from syscat.functions 
                where funcschema in( 'ADMINISTRATOR','F')"

#获取远程库的表，排除本地已存在的同名表，并写入到文件中
db2 $sql >table
#删除空白行
sed '/^[\t]*$/d' table >tables
#删除前2行
sed '1,2d' tables >table
#删除最后一行
sed '$d' table >tables
#更改文件名
mv tables content`date +%F`
rm table
