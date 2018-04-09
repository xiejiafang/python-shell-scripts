#!/bin/sh

#引入用户环境变量
source /home/db2inst/.bash_profile
set -x

#开始计时
start=`date +%s`

#删除2天前的备份
file="/home/db2inst/backup/`date -d -1day +%F`"
rm -rf $file*;


#创建文件
file="/home/db2inst/backup/`date +%F`"
if [[ -d $file || -f $file* ]];then
    rm -rf $file*;
fi
    mkdir -p $file;


#断开连接
db2 force applications all
db2set db2comm=""
db2stop force
db2start


#备份
startbackup=`date +%s`
db2 "backup database hnii to $file parallelism 8"

if [ $? == 0 ]; then
    endbackup=`date +%s` && run_time=$[ endbackup - startbackup ]; echo $run_time>$file/successful || run_time=0
    echo "backup successfully！";
else
    echo "backup failed!" >$file/error
fi

#清除IPC,防止出现启动错误
#ipclean

#恢复连接
db2set db2comm=tcpip
db2stop force
db2start


#结束记时
end=`date +%s`

#计算用时
run_time=$[ end - start ]
echo $run_time ;
set +x

