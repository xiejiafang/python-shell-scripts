#!/bin/sh
#记录开始时间
start=`date +%s`
#判断用户输入的参数是否完整
if [ $# != 1 ]; then
        echo "请输入表名!";
        exit
else
        var=$1;
fi
#连接数据库
#db2 connect to hnii
db2 "set current schema administrator"

#SQL语句
sql="select a_sum-b_sum from
        (select count(*) as a_sum
                from hnii0.$var) a,
        (select count(*) as b_sum
                from administrator.$var) b"

#统计行数差异
echo "--------------------------------------------------------------------------------------------------------";
echo "检查表差异:$var"
echo "--------------------------------------------------------------------------------------------------------";
db2 "select * from ($sql)a" |sed -n '4p'|awk '{print $1}'>row
while read row
do
        rowNew=$row
done <row
rm row
echo "两表之间行数之差：$rowNew行"

if [ $rowNew != 0 ]; then
	#定义游标
	echo "正在创建游标"
	db2 "declare c1 cursor for select * from hnii0.$var"

	#清空原数据
	echo "清空原表数据"
	db2 "alter table administrator.$var activate not logged initially with empty table"
	
	#如果清空失败，防止表挂起
	if [ $? != 0 ]; then
		db2 "load from aaaa.txt of del terminate into administrator.$var"
		if [ $? == 0 ]; then
			db2 "alter table administrator.$var activate not logged initially with empty table"
		fi
		rowNew=0;
	fi

	#从游标中插入数据
	echo "正在写入数据"
	db2 "load from c1 of cursor insert into administrator.$var  "|grep Number

	#统计行数
	db2 "select count(*) from $var" |sed -n '4p'|awk '{print $1}'>row
	while read row
	do
	        rowCount=$row
	done <row
	rm row

	#记录结束时间
	end=`date +%s`

	#计算时间差
	run_time=$[ end - start ]

	#记录信息到表info_processtables中
	db2 "insert into info.processtables values ('$var','C',$rowCount,$rowNew,$run_time,current timestamp)"
else
	echo "$var表无更新"

	#记录结束时间
	end=`date +%s`

	#计算时间差
	run_time=$[ end - start ]

	#记录信息到表info_processtables中
	db2 "insert into info.processtables values ('$var','=',0,0,$run_time,current timestamp)"
fi
