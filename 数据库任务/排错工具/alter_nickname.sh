#!/bin/sh

#引入用户环境变量
source /home/db2inst/.bash_profile

#开启跟踪
set -x

#连接本地库

#循环
while read table
do
    nickname=$(echo $table |awk '{print $1}')
    db2 "select count(*) from administrator.$nickname"
    if [ $? != 0 ]; then
        db2 "drop nickname administrator.$nickname"
        db2 "create nickname administrator.$nickname for tk_102.zhengzh.$nickname"
        continue
    fi
done <$1

#断开连接
db2 connect reset

#关闭跟踪
set +x
