#!/bin/sh
#定义命令路径
ph='/home/db2inst/sqllib/bin'

#引入用户环境变量
source /home/db2inst/.bash_profile

#开启跟踪
set -x

#连接本地库
isConnect='N'
while [ $isConnect = "N" ]
do
	if [ $? = 0 ]; then
		isConnect="Y"
	else
		sleep 20
	fi
done

#检查函数
function checkData(){
	#参数
        if [ $# -lt 1 ]; then
                echo "请输入参数！"
                return 1
        fi
	tablename=$1
	#判断列名
        case $1 in
                hn_rtrninfo)
                        colname="trandate"
                        ;;
                hn_acctinfo)
                        colname="trandate"
                        ;;
                *)
                        message="错误内容未定义。"
                        ;;
        esac
	#书写语句
	dayCount=`date -d yesterday +%d`
	#强制10进制,以0为开头的全部默认是八进制
	dayCount=$((10#$dayCount -0))
        errordays=""
        db2 " with mytables as(
	select distinct mod($colname,year(current date -1 days)*10000+month(current date -1 days)*100) as myday
        	from administrator.$tablename
                	where $colname between decimal(current date - day(current date -1 day) days) and decimal(current date -1 days)),
	daycount as(                
	select   row_number()over() as myday
        	from administrator.hn_cnttype fetch first $dayCount rows only)
	select myday||'日' from daycount 
	except 
	select myday||'日' from mytables " |sed  '1,3d' |sed '/^[\t]*$/d'|sed '$d'|awk '{print $1}' >check
        while read check
        do
                errordays="$errordays$check"
        done <check
        rm check
	#写入检查结果
	if [ -z $errordays ]; then
		return 0
	else
                message="[34数据库]$tablename缺少$errordays数据！"
                $ph/db2 "insert into hnii0.hn_sms_msg(SMSFL,HANDSETNO,SMSINFO,STATCODE,USERID,DS_TIME)
                select 'datacheck',handphone,'$message','0','XIE',date(current timestamp)
                        from administrator.sms_list where smsfl = 'CHECK' "
		result=$[result+1] 
	fi
}
#调用函数
result=0
checkData hn_rtrninfo
checkData hn_acctinfo

#判断结果
if [ $result = 0 ]; then
	message="[34数据库]数据逻辑检查正常！"
        $ph/db2 "insert into hnii0.hn_sms_msg(SMSFL,HANDSETNO,SMSINFO,STATCODE,USERID,DS_TIME)
        select 'datacheck',handphone,'$message','0','XIE',date(current timestamp)
                 from administrator.sms_list where smsfl = 'CHECK'"
        $ph/db2 "insert into hnii0.hn_sms_msg(SMSFL,HANDSETNO,SMSINFO,STATCODE,USERID,DS_TIME)
	select 
	    'datacheck',
	    handphone,
	    '[34数据库]\n'||
              (select 
  '受理保费:'
          ||'当日:'||trim(char(sum(case when hprrcvdt = decimal(current date -1 days) then acctamt end)/10000))
          ||'万;当月:'||trim(char(sum(case when hprrcvdt between decimal(current date - day(current date -1 days) days) and decimal(current date) then acctamt end)/10000))||'万;\n'
          from administrator.hn_ipe_hpad
              where agtype IN ('TA','SA','TS','SM','SD','SE','AS','SS','UM','AD','HD','RC')
              AND hprrcvdt between decimal(current date - day(current date -1 days) days) and decimal(current date))  ||
	    (select 
		  '预收业绩:'
		  ||'当日:'||trim(char(sum(case when trandate = decimal(current date -1 days) then acctamt end)/10000))
		  ||'万;当月:'||trim(char(sum(case when trandate between decimal(current date - day(current date -1 days) days) and decimal(current date) then acctamt end)/10000)) ||'万;\n'
		  from administrator.hn_rtrninfo
		      where trandate between year(current date)*10000+101 and decimal(current date - 1 days)
                     AND batc_type <>'CS'
              and agtype IN ('TA','SA','TS','SM','SD','SE','AS','SS','UM','AD','HD','RC') )||
	    (select 
		  '承保业绩:'
		  ||'当日:'||trim(char(sum(case when trandate = decimal(current date -1 days) then acctamt_std end)/10000))
		  ||'万;当月:'||trim(char(sum(case when trandate between decimal(current date - day(current date -1 days) days) and decimal(current date) then acctamt_std end)/10000))
		  ||'万;当年:'||trim(char(sum(acctamt_std)/10000))||'万;\n'
		  from administrator.hn_acctinfo
		      where trandate between year(current date)*10000+101 and decimal(current date)
              and agtype IN ('TA','SA','TS','SM','SD','SE','AS','SS','UM','AD','HD','RC') )||
              (select 
  '人力信息:'
          ||'当日新增:'||trim(char(count(case when dteapp = decimal(current date -1 days) then agntnum end)))
          ||'人;当前人力:'||trim(char(count(agntnum)))||'人'
	  from administrator.hn_agntinfo
	      where dtetrm=99999999
	      and agtype IN ('TA','SA','TS','SM','SD','SE','AS','SS','UM','AD','HD','RC')),              
	    '0',
	    'XIE',
	    date(current timestamp)
		 from administrator.sms_list where smsfl = 'CHECK'"
fi
#断开连接
$ph/db2 connect reset

#关闭跟踪
set +x
