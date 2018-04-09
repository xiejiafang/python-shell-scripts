#!/bin/sh

#引入用户环境变量
source /home/db2inst/.bash_profile

#开始计时
start=`date +%s`

#开启跟踪
#set -x

#连接本地库
db2 connect to hnii

#判断依赖表是否加工完成
isSuccess="N"
while [ $isSuccess = "N" ]
do
	sql="select case when count(*)>=2 then 'Y' else 'N' end
        	from info.taskmessage
                	where date(info_time)=current date and message='数据正常!' and tablename in('agntinfo','acctinfo')"
	result=$(db2 -x "$sql")
	isSuccess=$result
done

#################更新数据#################################
#更新acctinfo_year,只要每个月初1号更新
curr_day=$(date +%d)
if [ $curr_day == "13" ]; then
    db2 "refresh table administrator.hn_acctinfo_year"
fi
#更新acctinfo_month,只在每天的早上8点和12点更新
curr_hour=$(date +%H)
if [[ $curr_hour == "08" || $curr_hour == "12" ]]; then
    db2 "refresh table administrator.hn_acctinfo_month"
fi

#更新acctinfo_day，每半小时同步一次
db2 "refresh table administrator.hn_acctinfo_day"
db2 "commit"

#记录结束时间
end=`date +%s`

#计算时间差
echo "运行时长:"$[ end - start ]



#断开连接
db2 connect reset

#关闭跟踪
set +x
