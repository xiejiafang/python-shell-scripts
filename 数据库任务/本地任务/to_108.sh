#!/bin/sh

#引入用户环境变量
source /home/db2inst/.bash_profile

##开始计时
start=`date +%s`

#连接本地库
db2 connect to hnii

#决定加工年度业绩
curr_day=$(date +%d)
if [ $curr_day == "01" ]; then
	filename="ipe_acct_year"
	sql="
		export to $filename.txt of del
		select
		row_number() over(),
		branch,
		aracde,
		agntnum,
		trim(agntname) as agntname,
		agtype,
		chdrnum,
		trandate,
		cnttype,
		batc_type,
		sum(acctamt),
		sum(acctamt_std),
		sum(case when cnttype=ins then acctamt_std else 0 end) as primary_bf
		from administrator.hn_acctinfo
		where trandate between int(decimal(current date)/10000)*10000 and int(decimal(current date -1 month)/100)*100+31
		and agtype in(select agtype from administrator.hn_agtype)
		group by 
		branch,
		aracde,
		agntnum,
		trim(agntname),
		agtype,
		chdrnum,
		batc_type,
		trandate,
		cnttype
		"
	db2 -x "$sql" 
	scp $filename.txt it@10.19.19.108:/home/it
	rm $filename.txt

	#银保全年业绩
	filename="ipe_acct_bnk_year"
	sql="
		export to $filename.txt of del
		SELECT
		series,
		case when a.series in('FIC','TZF') then 'FIC' else a.branch end branch,
		bk_holding_name,
		a.chdrnum,
		trandate,
		a.cnttype,   
		a.ape,
		a.acctamt_std,
		a.acctamt_std_a,
		a.agntnum,
		a.agntname,
		b.stat
		FROM
		administrator.hn_ipe_acct_bnk a
		LEFT JOIN
		administrator.hn_ipe_chdrinfo_bnk b
		ON
		a.chdrnum=b.chdrnum
		WHERE
		trandate between 20170101 and int(decimal(current date -1 month)/100)*100+31
		and bnk_cls='高价值'
		"
	db2 -x "$sql" 
	scp $filename.txt it@10.19.19.108:/home/it
	rm $filename.txt
fi

#决定是否加工当日业绩
curr_hour=$(date +%H)
if [[ $curr_hour == "07" || $curr_hour == "10" ]]; then
	#emp
	filename="emp"
	sql="
		export to $filename.txt of del
		select 
		oa_num,
		ehr_num,
		job,
		name,
		sex,
		mobile,
		replace(replace(replace(aracde_name,'中心支公司',''),'河南',''),'营销本部','') aracde_name,
		depart_name,
		dc_name,
		dc_ip
		from hnii235.zh_userinfo
	"
	db2 -x "$sql" 
	scp $filename.txt it@10.19.19.108:/home/it
	rm $filename.txt

	#ipe_hpad
	filename="ipe_hpad"
	sql="
		export to $filename.txt of del
		select
		row_number() over(),
		branch,
		aracde,
		agntnum,
		trim(agntname) as agntname,
		agtype,
		chdrnum,
		hprrcvdt,
		cnttype,
		sum(acctamt),
		sum(acctamt_std)
		from administrator.hn_$filename
		where hprrcvdt between int(decimal(current date)/100)*100 and decimal(current date -1 day)
		and agtype in(select agtype from administrator.hn_agtype)
		group by 
		branch,
		aracde,
		agntnum,
		trim(agntname),
		agtype,
		chdrnum,
		hprrcvdt,
		cnttype
		"
	db2 -x "$sql" 
	scp $filename.txt it@10.19.19.108:/home/it
	rm $filename.txt

	#rtrninfo
	filename="ipe_rtrn"
	sql="
		export to $filename.txt of del
		select
		row_number() over(),
		branch,
		aracde,
		agntnum,
		trim(agntname) as agntname,
		agtype,
		chdrnum,
		trandate,
		cnttype,
		sum(acctamt),
		sum(acctamt_std)
		from administrator.hn_rtrninfo
		where trandate between int(decimal(current date)/100)*100 and decimal(current date -1 day)
		and agtype in(select agtype from administrator.hn_agtype)
		group by 
		branch,
		aracde,
		agntnum,
		trim(agntname),
		agtype,
		chdrnum,
		trandate,
		cnttype
		"
	db2 -x "$sql" 
	scp $filename.txt it@10.19.19.108:/home/it
	rm $filename.txt

	#acctinfo
	filename="ipe_acct"
	sql="
		export to $filename.txt of del
		select
		row_number() over(),
		branch,
		aracde,
		agntnum,
		trim(agntname) as agntname,
		agtype,
		chdrnum,
		trandate,
		cnttype,
		batc_type,
		sum(acctamt),
		sum(acctamt_std),
		sum(case when cnttype=ins then acctamt_std else 0 end) as primary_bf
		from administrator.hn_acctinfo
		where trandate between int(decimal(current date)/100)*100 and decimal(current date -1 day)
		and agtype in(select agtype from administrator.hn_agtype)
		group by 
		branch,
		aracde,
		agntnum,
		trim(agntname),
		agtype,
		chdrnum,
		trandate,
	 	batc_type,
		cnttype
		"
	db2 -x "$sql" 
	scp $filename.txt it@10.19.19.108:/home/it
	rm $filename.txt


	#agntinfo
	filename="agntinfo"
	sql="
		export to $filename.txt of del
		select
		row_number() over(),
		branch,
		aracde,
		case when partnum='' or partnum is null then '99999999' else partnum end as partnum,
		case when partnum='' or partnum is null then '无归属' else partname end partname,
		case when teamnum='' or teamnum is null then '99999999' else teamnum end teamnum,
		case when teamnum='' or teamnum is null then '无归属' else teamname end teamname,
		agntnum,
		agntname,
		case when agtype='RC' then xqtype else agtype end as agtype,
		dteapp,
		dtetrm
		from administrator.hn_agntinfo
		where dtetrm>=int(decimal(current date)/100)*100
		and agtype in(select agtype from administrator.hn_agtype)"
	db2 -x "$sql" 
	scp $filename.txt it@10.19.19.108:/home/it
	rm $filename.txt

	#银保当月业绩
	filename="ipe_acct_bnk"
	sql="
		export to $filename.txt of del
		SELECT
		series,
		case when a.series in('FIC','TZF') then 'FIC' else a.branch end branch,
		bk_holding_name,
		a.chdrnum,
		trandate,
		a.cnttype,   
		a.ape,
		a.acctamt_std,
		a.acctamt_std_a,
		a.agntnum,
		a.agntname,
		b.stat
		FROM
		administrator.hn_ipe_acct_bnk a
		LEFT JOIN
		administrator.hn_ipe_chdrinfo_bnk b
		ON
		a.chdrnum=b.chdrnum
		where trandate between int(decimal(current date)/100)*100 and decimal(current date -1 day)
		and bnk_cls='高价值'
		"
	db2 -x "$sql" 
	scp $filename.txt it@10.19.19.108:/home/it
	rm $filename.txt
	#agntinfo
	filename="agntinfo_bnk"
	sql="
		export to $filename.txt of del
		SELECT
		series,
		case when series in('FIC','TZF') then 'FIC' else branch end branch,
		agntnum,
		agntname,
		DTERM,
		dteapp
		from administrator.hn_ipe_agntinfo_bnk
		where DTERM>=int(decimal(current date - 1 year)/100)*100
	"
	db2 -x "$sql" 
	scp $filename.txt it@10.19.19.108:/home/it
	rm $filename.txt

fi

#ipe_hpad
filename="ipe_hpad_rt"
sql="
	export to $filename.txt of del
	select
	row_number() over(),
	branch,
	aracde,
	agntnum,
	trim(agntname) as agntname,
	agtype,
	chdrnum,
	hprrcvdt,
	cnttype,
	sum(acctamt),
	sum(acctamt_std)
	from administrator.hn_$filename
	where hprrcvdt = decimal(current date)
	and agtype in(select agtype from administrator.hn_agtype)
	group by 
	branch,
	aracde,
	agntnum,
	trim(agntname),
	agtype,
	chdrnum,
	hprrcvdt,
	cnttype
	"
db2 -x "$sql" 
scp $filename.txt it@10.19.19.108:/home/it
rm $filename.txt


#rtrninfo
filename="ipe_rtrn_rt"
sql="
	export to $filename.txt of del
	select
	row_number() over(),
	branch,
	aracde,
	agntnum,
	trim(agntname) as agntname,
	agtype,
	chdrnum,
	trandate,
	cnttype,
	sum(acctamt),
	sum(acctamt_std)
	from administrator.hn_rtrninfo
	where trandate = decimal(current date)
	and agtype in(select agtype from administrator.hn_agtype)
	group by 
	branch,
	aracde,
	agntnum,
	trim(agntname),
	agtype,
	chdrnum,
	trandate,
	cnttype
	"
db2 -x "$sql" 
scp $filename.txt it@10.19.19.108:/home/it
rm $filename.txt


#rtrninfo
filename="ipe_acct_rt"
sql="
	export to $filename.txt of del
	select
	row_number() over(),
	branch,
	aracde,
	agntnum,
	trim(agntname) as agntname,
	agtype,
	chdrnum,
	trandate,
	cnttype,
	batc_type,
	sum(acctamt),
	sum(acctamt_std),
	sum(case when cnttype=ins then acctamt_std else 0 end) as primary_bf
	from administrator.hn_acctinfo
	where trandate = decimal(current date)
	and agtype in(select agtype from administrator.hn_agtype)
	group by 
	branch,
	aracde,
	agntnum,
	trim(agntname),
	agtype,
	chdrnum,
	trandate,
	batc_type,
	cnttype
	"
db2 -x "$sql" 
scp $filename.txt it@10.19.19.108:/home/it
rm $filename.txt
#银保当月业绩
filename="ipe_acct_bnk_rt"
sql="
	export to $filename.txt of del
	SELECT
	series,
	case when a.series in('FIC','TZF') then 'FIC' else a.branch end branch,
	b.name,
	a.chdrnum,
	trandate,
	a.cnttype,   
	a.ape,
	a.acctamt_std,
	a.acctamt_std_a,
	a.agntnum,
	a.agntname,
	''	
	FROM
	administrator.hn_ipe_acct_bnk a left join administrator.bankinfo b on a.ara_name=b.code
	where trandate = decimal(current date)
	--and bnk_cls='高价值'
	"
db2 -x "$sql" 
scp $filename.txt it@10.19.19.108:/home/it
rm $filename.txt



#机构，不经常更新#################################################################################
#branch
filename="branch"
sql="
        export to $filename.txt of del
	select 
	branch,
	name
	from administrator.hn_branch
	where branch<>'D'
"
#db2 -x "$sql" 
#scp $filename.txt it@10.19.19.108:/home/it
#rm $filename.txt

#aracde
filename="aracde"
sql="
        export to $filename.txt of del
	select 
	branch,
	aracde,
	shortname
	from administrator.hn_aracde
	where branch<>'D'
	and agentflag='N'
"
#db2 -x "$sql" 
#scp $filename.txt it@10.19.19.108:/home/it
#rm $filename.txt

#原表数据，无新增则无更新#############################################################################
#aracde
filename="comments"
sql="
        export to $filename.txt of del
	select 
	id,
	report_id,
	user_name,
	comment,
	commit_time
	from db_33.comments
"
#db2 -x "$sql" 
#scp $filename.txt it@10.19.19.108:/home/it
#rm $filename.txt

#replies
filename="replies"
sql="
        export to $filename.txt of del
	select 
	id,
	comment_id,
	replied,
	commit_time
	from db_33.replies
"
#db2 -x "$sql" 
#scp $filename.txt it@10.19.19.108:/home/it
#rm $filename.txt

#taixx
filename="taixx"
sql="
        export to $filename.txt of del
	WITH
	basc_table AS
	(
	SELECT
	date_snap,
	COUNT(*) AS dl_rl
	FROM
	db102.TAIXX_DL_DETAIL
	WHERE
	dydl_count>0
	and date_snap>=int(decimal(current date)/100)*100
	GROUP BY
	date_snap
	)
	SELECT
	case when branch='I0' THEN '80' else branch end,
	case when aracde='I01' then '807' else aracde end,
	partnum,
	teamnum,
	agntnum,
	trim(agntname),
	'',
	'',
	dydl_count
	FROM
	db102.TAIXX_DL_DETAIL
	WHERE
	dydl_count>0
	AND date_snap=
	(
	SELECT
	date_snap
	FROM
	basc_table
	WHERE
	dl_rl=
	(
	SELECT
	MAX(dl_rl)
	FROM
	basc_table) fetch first 1 rows only)
"
db2 -x "$sql" 
scp $filename.txt it@10.19.19.108:/home/it
rm $filename.txt

#taixx_bnk
filename="taixx_bnk"
sql="
        export to $filename.txt of del
	WITH
	basc_table AS
	(
	SELECT
	date_snap,
	COUNT(*) AS dl_rl
	FROM
	db102.TAIXX_DL_DETAIL_bnk
	WHERE
	当月登录频次>0
	--and decimal(date_snap)>=int(decimal(current date)/100)*100
	GROUP BY
	date_snap
	)
	SELECT
	CASE
	WHEN series IN ('FIC',
	'TZF')
	THEN 'FIC'
	ELSE branch
	END AS branch,
	CASE
	WHEN series IN('SQ',
	'FIC')
	THEN 'SQ'
	WHEN series IN ('TZ',
	'TZF')
	THEN 'TZ'
	ELSE 'NO'
	END AS seires,
	substr(parth,1,8),
	substr(teamh,8,8),
	substr(b.agntnum,1,8),
	substr(b.agntname,1,30),
	'',
	'',
	当月登录频次
	FROM
	administrator.hn_ipe_agntinfo_bnk a ,
	db102.taixx_dl_detail_bnk b
	WHERE
	当月登录频次>0
	AND a.agntnum=b.agntnum
	AND date_snap=
	(
	SELECT
	date_snap
	FROM
	basc_table
	WHERE
	dl_rl=
	(
	SELECT
	MAX(dl_rl)
	FROM
	basc_table)
	FETCH
	FIRST 1 rows only)
	"
db2 -x "$sql" 
scp $filename.txt it@10.19.19.108:/home/it
rm $filename.txt

#ycb_hpad
filename="ycb_hpad"
sql="
	export to $filename.txt of del
	SELECT
	CASE
	WHEN branch='I0'
	THEN '80'
	ELSE branch
	END,
	CASE
	WHEN branch='I0'
	THEN '807'
	when agtype='RC' and aracde_xuqi<>'' then aracde_xuqi
	ELSE aracde
	END,
	agntnum,
	agtype,
	chdrnum,
	acctamt,
	txn_amt_st,
	cnttype,
	ycb_date,
	hprrcvdt,
	birth_flag
	FROM
	db102.ipe_acct_hpad_rt
	WHERE
	hprrcvdt BETWEEN 20171122 AND DECIMAL(CURRENT DATE)
	AND prepsign>0
	AND statcode NOT IN('WD',
	'PC')
	AND agtype IN
	(
	SELECT
	agtype
	FROM
	administrator.hn_agtype) with ur
	"
#db2 -x "$sql" 
#scp $filename.txt it@10.19.19.108:/home/it
#rm $filename.txt


#ycb_rtrn
filename="ycb_rtrn"
sql="
        export to $filename.txt of del
	SELECT
	CASE
	WHEN branch='I0'
	THEN '80'
	ELSE branch
	END,
	CASE
	WHEN branch='I0'
	THEN '807'
	when agtype='RC' and aracde_xuqi<>'' then aracde_xuqi
	ELSE aracde
	END,
	agntnum,
	agtype,
	chdrnum,
	acctamt,
	txn_amt_st,
	cnttype,
	ycb_date,
	hprrcvdt,
	birth_flag
	FROM
	db102.ipe_acct_hpad_rt
	WHERE
	hprrcvdt BETWEEN 20171122 AND DECIMAL(CURRENT DATE)
	AND prepsign>0
	and trandate >= 20171122
	and src_table='RTRN' 
	AND statcode NOT IN('WD',
	'PC')
	AND agtype IN
	(
	SELECT
	agtype
	FROM
	administrator.hn_agtype) with ur

"
#db2 -x "$sql" 
#scp $filename.txt it@10.19.19.108:/home/it
#rm $filename.txt



#ycb
filename="ycb"
sql="
        export to $filename.txt of del
	select
	case when branch='I0' then '80' else branch end,
	case when branch='I0' then '807' when agtype='RC' and aracde_xuqi<>'' then aracde_xuqi else aracde end,
	agntnum,
	agtype,
	chdrnum,
	acctamt,
	txn_amt_st,
	cnttype,
	ycb_date,
	hprrcvdt,
	trandate,
	birth_flag,
	statcode,
	case when src_table='RTRN' then 'Y' else '' end
	from  db102.ipe_acct_hpad_rt
	where hprrcvdt between 20171122 and decimal(current date)
	and prepsign>0
	and statcode not in('WD','PC')
	"
#db2 -x "$sql" 
#scp $filename.txt it@10.19.19.108:/home/it
#rm $filename.txt

#ycb_bnk
filename="ycb_bnk"
sql="
        export to $filename.txt of del
	select
	case when branch='I0' then '80' else branch end,
	bk_holding_name,
	agntnum,
	agtype,
	chdrnum,
	acctamt,
	txn_amt_st,
	cnttype,
	ycb_date,
	hprrcvdt,
	trandate,
	statcode,
	case when src_table='RTRN' then 'Y' else '' end
	from  administrator.ipe_acct_hpad_bnk_rt
	where hprrcvdt between 20171212 and decimal(current date)
	--and prepsign='1' 
	--and src_table='RTRN' 
	--and statcode not in('WD','PC')
"
#db2 -x "$sql" 
#scp $filename.txt it@10.19.19.108:/home/it
#rm $filename.txt


#hdrl_aracde
filename="hdrl_aracde"
sql="
	export to $filename.txt of del
	select
	*
	from administrator.hn_gx_kpi_rl
	where kpi_name in('3000P人力') 
"
db2 -x "$sql" 
scp $filename.txt it@10.19.19.108:/home/it
rm $filename.txt


#epolicy_cvr
filename="epolicy_cvr"
sql="
	export to $filename.txt of del
	select
	case when subcompany='I0' then '80' else substr(subcompany,1,2) end as branch,
	case when branchcode='I01' then '807' else substr(branchcode,1,3) end as aracde,
	substr(chdrnum,1,8),
	txn_amt_yr as acctamt,
	txn_amt_st as acctamt_std,
	substr(replace(agt_lgcy_nbr,'D',''),1,8) as agntnum,
	substr(agentlevel,1,2) as agtype,
	createtime
	from db102.epolicy_cvr a
	where lgcy_prod_cod='RRHS'
	and date(createtime)>='2018-2-9'
	and exists(select * from administrator.hn_ipe_hpad b where a.chdrnum=b.chdrnum)
"
db2 -x "$sql" 
scp $filename.txt it@10.19.19.108:/home/it
rm $filename.txt











#调用远程脚本
echo "调用远程脚本"
ssh it@10.19.19.108 "/home/it/load_data.sh"

#断开连接
db2 connect reset

