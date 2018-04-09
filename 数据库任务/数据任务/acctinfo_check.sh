#!/bin/sh
#定义命令路径
ph='/home/db2inst/sqllib/bin'

#引入用户环境变量
source /home/db2inst/.bash_profile


#开启跟踪
#set -x

#连接本地库
$ph/db2 connect to hnii
#判断是否有昨天的业绩
result=`db2 "select case when max(trandate)=decimal(current date -1 days) then 'YYY' else 'NNN' end as check_result from administrator.hn_acctinfo where trandate < decimal(current date)"`
result=`expr index "$result" "Y"`
if [ $result -eq 0 ]; then
    #如果没有
    echo "本地表缺少业绩，正在检查总公司的数据..."
    #循环检查总公司表中是否含有昨天的业绩
    for((i=1;i<=60;i++));do 
        result=$(db2 -x "select case when max(trandate)=decimal(current date -1 days) then 'YYY' else 'NNN' end as check_result from administrator.ipe_acct where trandate < decimal(current date)")
        if [ $result == "NNN" ]; then
            #如果也没有,则暂时10分钟
    	    echo "总公司也缺少业绩,等待10分钟后检查..."
            sleep 600;
	    continue;
        else
            #如果有业绩了
	    echo "准备更新业绩..."
            source ~/数据库任务/数据任务/chdrslinfo.sh
            source ~/数据库任务/数据任务/rtrninfo.sh
            source ~/数据库任务/数据任务/acctinfo.sh
            source ~/数据库任务/本地任务/checkdata.sh
	    echo "更新完毕."
            break
        fi
    done
else
    echo "业绩已是最新"
fi
    
#断开连接
$ph/db2 connect reset

#关闭跟踪
#set +x
