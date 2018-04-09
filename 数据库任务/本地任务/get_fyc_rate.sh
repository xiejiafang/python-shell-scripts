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
	$ph/db2 connect to hnii
	if [ $? = 0 ]; then
		isConnect="Y"
	else
		sleep 20
	fi
done

db2 "alter table administrator.hn_bonustable activate not logged initially with empty table"
db2 "declare c1 cursor for select * from hnii235.hn_bonustable where acctyear=year(current date)"
#防止"3"错误
db2 "load from test.txt of del terminate into administrator.hn_bonustable"
#加工数据
db2 "load from c1 of cursor insert into administrator.hn_bonustable"
db2 "UPDATE administrator.HN_bonustable a SET occdate=(SELECT period FROM administrator.hn_acctinfo b
					WHERE trandate BETWEEN year(current date)*10000 AND decimal(current date) 
						AND a.chdrnum=b.chdrnum AND a.CRTABLE=b.INSS FETCH first 1 ROWS only)
     WHERE acctyear=year(current date)"

#清空表
db2 "alter table administrator.hn_fyc_rate activate not logged initially with empty table"
db2 "insert into administrator.hn_fyc_rate
with mytable as(
select distinct
        a.crtable,
        b.period,
        b.freq,
        case when acctamt_std>0 then decimal(corigamt/acctamt,10,5) end as rate
        from administrator.hn_bonustable a,administrator.hn_acctinfo b
                where a.chdrnum=b.chdrnum 
                and a.acctyear>=year(current date -1 year) 
                and b.trandate between year(current date -1 year)*10000+0101 and decimal(current date)
                and a.origamt=b.acctamt
                and b.acctamt_std>0)
select 
        crtable,
        period,
        freq,
        max(rate) as rate
                from mytable
                        group by 
                                crtable,
                                period,
                                freq "


#断开连接
$ph/db2 connect reset

#关闭跟踪
set +x
