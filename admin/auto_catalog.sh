#!/bin/sh

#判断用户输入的参数是否完整
if [ $# != 4 ]; then
	echo "请输入要连接的远程主机IP地址、数据库名称、用户名和密码，用空格分隔!";
	exit
else
	ipaddr=$1;
	dbname=$2;
	user=$3;
	password=$4;
fi

#再次确认信息
echo "您输入的信息是：$1,$2,$3,$4";
read -p "确认吗？[y/n]:" answer

#判断用户选择
if [ $answer == "y" ]; then
	#建立node节点，远程端口号默认为50000，如果不同，请手工修改
	echo "建立节点..."
	db2 catalog tcpip node $dbname901 remote $ipaddr server 50000
	#建立节点数据库
	echo "创建节点数据库..."
	db2 catalog db $dbname901 as $dbname901 at node $dbname901
	#测试远程节点数据库是否可以正常连接
	echo "连接节点 $dbname 上的数据库 $dbname..."
	db2 connect to $dbname901 user $user using $password
	#断开连接
	db2 terminate
	#连接本地数据库
	read -p "请输入本地数据库名称：" localdb
	echo "连接到$localdb..."
	db2 connect to $localdb
	#激活联邦支持
	echo "激活联邦支持"
	db2 update database manager configuration using federated yes
	echo "创建包装器"
	db2 create wrapper drda;
	echo "创建服务器定义"
	read -p "请输入远程主机DB2的版本号：" version
	db2 "create server $dbname901 type db2/udb version $version wrapper drda authid \"$user\" password \"$password\" options(dbname \"$dbname901\")"
	echo "创建用户映射"
	db2 "create user mapping for user server $dbname901 options(remote_authid '$user',remote_password '$password')"

else
	echo "退出！";
	exit 0;
fi
