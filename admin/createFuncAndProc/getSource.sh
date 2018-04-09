#!/bin/sh
echo "本脚本可以自动将源数据库的函数、存储过程、视图等迁移到目标数据库，请根据提示输入相应信息。"

#提示输入迁移类型
read -p "准备迁移的是[view\function\procedure]:" answer
type=$answer;

#源数据库名称
read -p "源数据库在本地的昵称:" source;
dbSource=$source;

#本地数据库名称
read -p "本地数据库:" local;
dbLocal=$local;

#确认信息
read -p "即将从$dbSource上迁移$answer到$dbLocal。[y/n]:" yesOrNo;

if [ $yesOrNo == y ]; then
	db2 "set current schema administrator";
	
	#设置列名
	if [ $type == view ]; then
		mySchema="viewschema";
		mySelect="text";
		type="views";
		colnum="viewschema,viewname";
	elif [ $type == function ]; then
		mySchema="funcschema";
		mySelect="body";
		type=functions;
		colnum="funcschema,funcname";
	elif [ $type == procedure ]; then
		mySchema="procschema";
		mySelect="text";
		type="procedures";
		colnum="procschema,procname";
	else
		echo "错误的操作：$type";
		exit;
	fi;
	
	#SQL语句
	sql="select $mySelect ||'@'
        	from $dbSource.syscat_$type 
                	where $mySchema in('ADMINISTRATOR','F','P','V','VIEW')"
	#执行查询，并写入到文件中
	db2 $sql |tee table
	#删除空白行
	sed '/^[\t]*$/d' table >tables
	#删除前2行
	sed '1,1d' tables >table
	#删除最后一行
	sed '$d' table >tables
	#更改文件名
	mv tables List`date +%F`
	rm table

	#执行创建语句
	db2 -td@ -vf List`date +%F`|tee create.info;
	rm List`date +%F`;
	
	#核对SQL语句
	echo "以下是创建失败的：";
	sql="select $colnum 
        	from $dbSource.syscat_$type 
                	where $mySchema in('ADMINISTRATOR','F','P','V','VIEW')
	except
	select $colnum
		from syscat.$type
                	where $mySchema in('ADMINISTRATOR','F','P','V','VIEW')"
			
			
	#执行查询，并写入到文件中
	db2 $sql |tee failed.info;

elif [ $yesOrNo == n ]; then
	echo "no";
else
	echo "other";
fi;
