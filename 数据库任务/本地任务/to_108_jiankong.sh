#!/bin/sh

#引入用户环境变量
source /home/db2inst/.bash_profile

##开始计时
start=`date +%s`

#连接本地库
db2 connect to hnii


#tem63
filename="tem63"
sql="
	export to $filename.txt of del
	select
	flr,
	tmp,
	dew,
	hum,
	substr(tim,1,16)
	from jiankong60.Tem63
	where tim between current timestamp - 2 hours and current timestamp
	"
db2 -x "$sql" 
scp $filename.txt it@10.19.19.108:/home/it
rm $filename.txt

#apc
filename="apc"
sql="
	export to $filename.txt of del
	select
	case when host='11' then '11楼ups'
	when host='7F' then '7楼ups'
	when host='1K' then '13楼10k'
	when host='2K' then '13楼20k'
	when host='13' then '13楼10k-2'
	when host='10' then '郑州'
	when host='20' then '濮阳'
	when host='30' then '安阳'
	when host='40' then '南阳'
	when host='60' then '新乡'
	when host='70' then '平顶山'
	when host='80' then '焦作'
	when host='90' then '许昌'
	when host='A0' then '开封'
	when host='B0' then '商丘'
	when host='E0' then '驻马店'
	when host='F0' then '鹤壁'
	when host='G0' then '信阳'
	when host='D1' then '电销ups1'
	when host='D2' then '电销ups2'
	when host='DX' then '洛阳电销'
	end name,
	vol_in,
	vol_out,
	substr(maketime,1,16)
	from jiankong60.apc
	where host in ('11','7F','1K','2K','10','20','30','40','60','70','80','90','A0','B0','E0','F0','G0','D1','D2','DX','13')
	and maketime in (select max(maketime) from jiankong60.apc group by host)
	"
db2 -x "$sql" 
scp $filename.txt it@10.19.19.108:/home/it
rm $filename.txt


#nodes
filename="nodes63"
sql="
	export to $filename.txt of del
	select trim(caption),
	case when status=1 then 'up' when status=2 then 'down' end from jiankong60.nodes63
	where vendor IN('cisco','H3C')"
db2 -x "$sql" 
scp $filename.txt it@10.19.19.108:/home/it
rm $filename.txt


#调用远程脚本
ssh it@10.19.19.108 "/home/it/jiankong.sh"

#断开连接
db2 connect reset

