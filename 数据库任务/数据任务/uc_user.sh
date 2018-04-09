#!/bin/sh
#定义命令路径
ph='/home/db2inst/sqllib/bin'

#引入用户环境变量
source /home/db2inst/.bash_profile

#开始计时
start=`date +%s`

#开启跟踪
set -x

#函数
function checkState(){
	if [ $# -lt 2 ]; then
		echo "请输入2参数！"
		return 1
	fi
	case $2 in
		1)
			message="连接总公司数据库连续10次失败！"
			;;
		2)
			message="定义游标失败，10秒后重试！"
			;;
		3)	
			message="数据存在差异，10秒后重新加工！"
			;;
		4)
			message="数据正常!"
			;;
		*)
			message="错误内容未定义。"
			;;
	esac
	[ -z $run_time ] && run_time=0 
	$ph/db2 "insert into info.taskmessage values ('$1',current timestamp,'$message',$run_time)"
}


#连接本地库

#下载数据
isSuccess="N"
getTime=0
while [ $isSuccess != "Y" ]
do
	#清空表
	db2 "alter table administrator.hn_uc_user activate not logged initially with empty table"
	#加工数据
	db2 "insert into administrator.hn_uc_user              
	SELECT         
	        a.username,
	        name,
		b.uri,
	        B.groupname,
		d.uri,
	        D.GROUPNAME,
	        ENCRYPTEDPASSWORD,
	        mobile,
	        phone,
	        0
	        FROM uc16.JIVEUSER A,uc16.JIVEGROUP B,uc16.JIVEGROUPUSER C,uc16.JIVEGROUP D
                	WHERE A.USERNAME=C.USERNAME AND C.GROUPURI=B.URI AND B.PGROUPID=D.URI
                	AND C.ORD=-1 "
	if [ $? = 0 ]; then 
		isSuccess="Y"
	fi
	if [ $isSuccess = "Y" ]; then
		db2 "update administrator.hn_uc_user set manager=1
                		where branch='河南分公司'
                		and username in('zrx','songjunfeng','wuxiaogen','wangluzz','niudongming','fanzhiyong',
				'miyaozhong','lerui','baibing','jipengfei','chenxlzz','likaiming','yupenghui','dingjianlin',
                		'lihuan','hanzhifeng','zhangyongqiang','liushufeng','','','','','','')"
		db2 "delete from wx53.hn_uc_user"
		db2 "insert into wx53.hn_uc_user select * from administrator.hn_uc_user"
		#记录结束时间
        	end=`date +%s`
        	#计算时间差
	        run_time=$[ end - start ]
		checkState uc_user 4
	else
		checkState uc_user 3
                getTime=$[getTime+1]
                if [ $getTime -ge 3 ]; then
                        break
                fi
		sleep 120
	fi
done

#断开连接
$ph/db2 connect reset

#关闭跟踪
set +x
