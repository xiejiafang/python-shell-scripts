#!/bin/sh
#引入用户环境变量
source /home/db2inst/.bash_profile
ph="/home/db2inst/admin/createTable"

#connect to database;
db2 connect to hnii

#检查是否有新增表
echo "---------------------------------------------------------------------------------------------------------";
echo "检查新增表：";
echo "---------------------------------------------------------------------------------------------------------";
source $ph/getTableList.sh 1
while read line
do
	echo "正在添加新表：$line"
	source $ph/getTableData.sh $line
done < tableList`date +%F`

#检查现有表是否有更新
echo "---------------------------------------------------------------------------------------------------------";
echo "检查表更新：";
echo "---------------------------------------------------------------------------------------------------------";
source $ph/getTableList.sh 0
while read line
do
	source $ph/diffTableData.sh $line
done < tableList`date +%F`
db2 connect reset
rm tableList`date +%F`
exit
