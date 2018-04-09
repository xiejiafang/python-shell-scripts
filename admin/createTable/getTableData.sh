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
#db2 connect to hnii
db2 "set current schema administrator"

#创建表昵称
db2 "create nickname hnii0.$var for hnii0.administrator.$var ">/dev/null
if [ $? == 0 ];then
	echo "创建表昵称：hnii0.$var 成功！"
else
	echo "创建表昵称：hnii0.$var 失败,该昵称或已存在！"
fi

#创建表
db2 "create table $var like hnii0.$var in tbs_data index in tbs_index">/dev/null
if [ $? == 0 ];then
	echo "创建表：$var 成功！"
else
	echo "创建表：$var 失败,该表或已存在！"
fi

#定义游标
echo "正在创建游标"
db2 "declare c1 cursor for select * from hnii0.$var"

if [ $? == 0 ]; then
	#清空原数据
	echo "清空原表数据"
	db2 "alter table administrator.$var activate not logged initially with empty table"

	#从游标中插入数据
	echo "正在写入数据"
	db2 "load from c1 of cursor insert into administrator.$var  "|grep Number

	#启用压缩 
	echo "启用压缩"
	db2 "alter table $var compress yes"
	
	#重组表
	echo "重组表"
	db2 "reorg table $var"

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
	echo "插入执行信息到表info.processtables"
	db2 "insert into info.processtables values ('$var','A',$rowCount,0,$run_time,current timestamp)"
else
	echo "定义游标出错。"
fi
