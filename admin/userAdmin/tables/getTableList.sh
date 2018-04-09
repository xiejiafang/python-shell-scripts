#!/bin/sh
#判断用户输入的参数是否完整
if [ $# != 1 ]; then
        echo "请输入参数：0代表全部表，1代表新增表!";
        exit
else
        var=$1;
fi
#db2 connect to hnii
db2 "set current schema administrator"
#SQL语句
if [ $var == 1 ];then
sql="select TABNAME 
        from hnii0.syscat_tables 
                where tabschema = 'ADMINISTRATOR' and type = 'T'
except
select tabname 
        from syscat.tables 
                where tabschema = 'ADMINISTRATOR' and type = 'T' "
elif [ $var == 0 ];then
sql="select TABNAME 
        from syscat.tables
                where tabschema = 'ADMINISTRATOR' and type = 'T'"
else
	echo "参数错误！请重新运行。"
	exit
fi
#获取远程库的表，排除本地已存在的同名表，并写入到文件中
db2 $sql >table
#删除空白行
sed '/^[\t]*$/d' table >tables
#删除前2行
sed '1,2d' tables >table
#删除最后一行
sed '$d' table >tables
#更改文件名
mv tables tableList`date +%F`
rm table
