create  procedure p.get_ybssyj()
begin atomic
--拓展当天预收数据
delete from administrator.hn_ybyj where trandate = year(date(current timestamp))*10000+month(date(current timestamp))*100
+day(date(current timestamp));
insert into administrator.hn_ybyj
select char('TZ') as series,branch,aracde,chdrnum,cnttype,acctamt,acctamt_std,'' as freq,0 as period,batc_type as statcode,
agntnum,agntname as name,trandate,'g' as sort,teamnum,teamname,'rt' as statcode2
        from administrator.ipe_rtrn_rt
                where agtype IN ('BY','BZ') and trandate = year(date(current timestamp))*10000+month(date(current timestamp))*100
                +day(date(current timestamp))
union all
--拓展当天承保数据
select char('TZ') as series,branch,aracde,chdrnum,cnttype,acctamt,acctamt_std,'' as freq,0 as period,batc_type as statcode,
agntnum,agntname as name,trandate,'g' as sort,teamnum,teamname,'ac' as statcode2
        from administrator.ipe_acct_rt
                where agtype IN ('BY','BZ') and trandate = year(date(current timestamp))*10000+month(date(current timestamp))*100
                +day(date(current timestamp))
union all
--拓展个险业绩，因这部分业绩均用信贷无忧手工单，故关连zcdrpf_load用保单号区分
select char('TZ') as series,a.branch,a.aracde,a.chdrnum,a.cnttype,acctamt,acctamt_std,'' as freq,0 as period,a.batc_type as statcode,
c.reportag01 as agntnum,d.name,a.trandate,'g' as sort,e.agency_group as teamnum,e.group_name as teamname,'rt' as statcode2
        from administrator.ipe_rtrn_rt a left join administrator.zcdrpf_load c on a.chdrnum=c.chdrnum 
        left join administrator.agency d on c.reportag01=d.agency_code left join administrator.agency_group e on
        d.agency_group = e.agency_group 
                where agtype IN ('BA') and (c.reportag01 like '808%' or  c.reportag01 like '%A')
                and trandate = year(date(current timestamp))*10000+month(date(current timestamp))*100
                +day(date(current timestamp))
union all
--拓展个险业绩，因这部分业绩均用信贷无忧手工单，故关连zcdrpf_load用保单号区分
select char('TZ') as series,a.branch,a.aracde,a.chdrnum,a.cnttype,acctamt,acctamt_std,'' as freq,0 as period,a.batc_type as statcode,
c.reportag01 as agntnum,d.name,a.trandate,'g' as sort,e.agency_group as teamnum,e.group_name as teamname,'ac' as statcode2
        from administrator.ipe_acct_rt a left join administrator.zcdrpf_load c on a.chdrnum=c.chdrnum 
        left join administrator.agency d on c.reportag01=d.agency_code left join administrator.agency_group e on
        d.agency_group = e.agency_group 
                where agtype IN ('BA') and (c.reportag01 like '808%' or  c.reportag01 like '%A')
                and trandate = year(date(current timestamp))*10000+month(date(current timestamp))*100
                +day(date(current timestamp))
union all
--FIC当天预收数据
select char('FIC') as series,branch,aracde,chdrnum,cnttype,acctamt,acctamt_std,'' as freq,0 as period,batc_type as statcode,
agntnum,agntname as name,trandate,'g' as sort,teamnum,teamname,'rt' as statcode2
        from administrator.ipe_rtrn_rt
                where agtype IN ('BC','BD','BM','BP') and trandate = year(date(current timestamp))*10000+month(date(current timestamp))*100
                +day(date(current timestamp))
union all
--FIC当天承保数据
select char('FIC') as series,branch,aracde,chdrnum,cnttype,acctamt,acctamt_std,'' as freq,0 as period,batc_type as statcode,
agntnum,agntname as name,trandate,'g' as sort,teamnum,teamname,'ac' as statcode2
        from administrator.ipe_acct_rt
                where agtype IN ('BC','BD','BM','BP') and trandate = year(date(current timestamp))*10000+month(date(current timestamp))*100
                +day(date(current timestamp))
union all
--FIC个险业绩，因这部分业绩均用信贷无忧手工单，故关连zcdrpf_load用保单号区分
select char('FIC') as series,a.branch,a.aracde,a.chdrnum,a.cnttype,acctamt,acctamt_std,'' as freq,0 as period,a.batc_type as statcode,
c.reportag01 as agntnum,d.name,a.trandate,'g' as sort,e.agency_group as teamnum,e.group_name as teamname,'rt' as statcode2
        from administrator.ipe_rtrn_rt a left join administrator.zcdrpf_load c on a.chdrnum=c.chdrnum 
        left join administrator.agency d on c.reportag01=d.agency_code left join administrator.agency_group e on
        d.agency_group = e.agency_group 
		where reportag01 in
                        (select agency_code from administrator.hn_agency where agency_group
                                IN ('08900069','08900070','08900071','08900074'))  and agtype IN ('BA')
                and trandate = year(date(current timestamp))*10000+month(date(current timestamp))*100
                +day(date(current timestamp))
union all
--FIC个险业绩，因这部分业绩均用信贷无忧手工单，故关连zcdrpf_load用保单号区分
select char('FIC') as series,a.branch,a.aracde,a.chdrnum,a.cnttype,acctamt,acctamt_std,'' as freq,0 as period,a.batc_type as statcode,
c.reportag01 as agntnum,d.name,a.trandate,'g' as sort,e.agency_group as teamnum,e.group_name as teamname,'ac' as statcode2
        from administrator.ipe_acct_rt a left join administrator.zcdrpf_load c on a.chdrnum=c.chdrnum 
        left join administrator.agency d on c.reportag01=d.agency_code left join administrator.agency_group e on
        d.agency_group = e.agency_group 
		where reportag01 in
                        (select agency_code from administrator.hn_agency where agency_group
                                IN ('08900069','08900070','08900071','08900074'))  and agtype IN ('BA')
                and trandate = year(date(current timestamp))*10000+month(date(current timestamp))*100
                +day(date(current timestamp))
union all
--首期个险业绩
select char('SQ') as series,a.branch,a.aracde,a.chdrnum,a.cnttype,acctamt,acctamt_std,'' as freq,0 as period,a.batc_type as statcode,
c.reportag01 as agntnum,d.name,a.trandate,'g' as sort,e.agency_group as teamnum,e.group_name as teamname,'rt' as statcode2
        from administrator.ipe_rtrn_rt a left join administrator.zcdrpf_load c on a.chdrnum=c.chdrnum 
        left join administrator.agency d on c.reportag01=d.agency_code left join administrator.agency_group e on
        d.agency_group = e.agency_group 
		where reportag01 in
                        (select agency_code from administrator.hn_agency where agency_group
                        IN ('08900069','08900070','08900071','08900074')) and reportag01 not like '808%'
                and reportag01 not like '%A'  and agtype IN ('BA')
                and trandate= year(date(current timestamp))*10000+month(date(current timestamp))*100
                +day(date(current timestamp))
union all
--首期当天承保业绩
select char('SQ') as series,a.branch,a.aracde,a.chdrnum,a.cnttype,acctamt,acctamt_std,'' as freq,0 as period,a.batc_type as statcode,
c.reportag01 as agntnum,d.name,a.trandate,'g' as sort,e.agency_group as teamnum,e.group_name as teamname,'ac' as statcode2
        from administrator.ipe_acct_rt a left join administrator.zcdrpf_load c on a.chdrnum=c.chdrnum 
        left join administrator.agency d on c.reportag01=d.agency_code left join administrator.agency_group e on
        d.agency_group = e.agency_group 
		where reportag01 in
                        (select agency_code from administrator.hn_agency where agency_group
                        IN ('08900069','08900070','08900071','08900074')) and reportag01 not like '808%'
                and reportag01 not like '%A'  and agtype IN ('BA')
                and trandate= year(date(current timestamp))*10000+month(date(current timestamp))*100
                +day(date(current timestamp))
union all
--无法区分首续期的情况
select case when reportag01 is null then 'NO' end  as series,a.branch,a.aracde,a.chdrnum,a.cnttype,acctamt,acctamt_std,'' as freq,0 as period,a.batc_type as statcode,
case when c.reportag01 is null then a.agntnum end as agntnum,case when d.name is null
then substr(trim(a.agntname),1,13) end as name ,a.trandate,'g' as sort,e.agency_group as teamnum,e.group_name as teamname,'ac' as statcode2
        from administrator.ipe_acct_rt a left join administrator.zcdrpf_load c on a.chdrnum=c.chdrnum 
        left join administrator.agency d on c.reportag01=d.agency_code left join administrator.agency_group e on
        d.agency_group = e.agency_group 
                where agtype IN ('BA')  and reportag01 is null 
                and trandate = year(date(current timestamp))*10000+month(date(current timestamp))*100
                +day(date(current timestamp))
union all
select case when reportag01 is null then 'NO' end  as series,a.branch,a.aracde,a.chdrnum,a.cnttype,acctamt,acctamt_std,'' as freq,0 as period,a.batc_type as statcode,
case when c.reportag01 is null then a.agntnum end as agntnum,case when d.name is null
then substr(trim(a.agntname),1,13) end as name ,a.trandate,'g' as sort,e.agency_group as teamnum,e.group_name as teamname,'rt' as statcode2
        from administrator.ipe_rtrn_rt a left join administrator.zcdrpf_load c on a.chdrnum=c.chdrnum 
        left join administrator.agency d on c.reportag01=d.agency_code left join administrator.agency_group e on
        d.agency_group = e.agency_group 
                where agtype IN ('BA')  and reportag01 is null 
                and trandate = year(date(current timestamp))*10000+month(date(current timestamp))*100
                +day(date(current timestamp))
union all
--拓展银保业绩
select char('TZ') as series,c.branch,substr(a.station_a,3,2) as aracde,bm_cert as chdrnum,kinds as cnttype,premium as acctamt,premium as acctamt_std,
char(jf_way) as freq,smallint(jfyears) as period,app_flag as statcode,a.agency_code as agntnum,d.name,
administrator.date_int(a.make_time) as trandate,'y' as sort,e.agency_group as teamnum,e.group_name as teamname,'' as statcode2
        from administrator.rta19 a,administrator.hn_branch c,administrator.hn_agency d,administrator.hn_agency_group e
                where  a.station = c.station and a.agency_code=d.agency_code  and d.agency_group = e.agency_group
                and app_flag IN ('1','WT')  and make_time=DATE(current timestamp)
                and (a.agency_code like '808%' or  a.agency_code like '%A')
union all
select char('TZ') as series,c.branch,substr(a.station_a,3,2) as aracde,bm_cert as chdrnum,kinds as cnttype,-premium as acctamt,-premium as acctamt_std,
char(jf_way) as freq,smallint(jfyears) as period,app_flag as statcode,a.agency_code as agntnum,d.name,
administrator.date_int(a.rq_make) as trandate,'y' as sort,e.agency_group as teamnum,e.group_name as teamname,'' as statcode2
        from administrator.rta1b9 a,administrator.hn_branch c,administrator.hn_agency d,administrator.hn_agency_group e
                where  a.station = c.station and a.agency_code=d.agency_code  and d.agency_group = e.agency_group
                and pg_type in ('WT','BT')  and a.rq_make=DATE(current timestamp)
                and (a.agency_code like '808%' or  a.agency_code like '%A')
union all
--FIC银保业绩
select char('FIC') as series,c.branch,substr(a.station_a,3,2) as aracde,bm_cert as chdrnum,kinds as cnttype,premium as acctamt,premium as acctamt_std,
char(jf_way) as freq,smallint(jfyears) as period,app_flag as statcode,a.agency_code as agntnum,d.name,
administrator.date_int(a.make_time) as trandate,'y' as sort,e.agency_group as teamnum,e.group_name as teamname,'' as statcode2
        from administrator.rta19 a,administrator.hn_branch c,administrator.hn_agency d,administrator.hn_agency_group e
                where  a.station = c.station and a.agency_code=d.agency_code  and d.agency_group = e.agency_group
                and app_flag IN ('1','WT')  and make_time=DATE(current timestamp)
                and a.agency_group IN ('08900069','08900070','08900071')
union all
select char('FIC') as series,c.branch,substr(a.station_a,3,2) as aracde,bm_cert as chdrnum,kinds as cnttype,-premium as acctamt,-premium as acctamt_std,
char(jf_way) as freq,smallint(jfyears) as period,app_flag as statcode,a.agency_code as agntnum,d.name,
administrator.date_int(a.rq_make) as trandate,'y' as sort,e.agency_group as teamnum,e.group_name as teamname,'' as statcode2
        from administrator.rta1b9 a,administrator.hn_branch c,administrator.hn_agency d,administrator.hn_agency_group e
                where  a.station = c.station and a.agency_code=d.agency_code  and d.agency_group = e.agency_group
                and pg_type in ('WT','BT')  and a.rq_make=DATE(current timestamp)
                and a.agency_group IN ('08900069','08900070','08900071')
union all
--首期银保业绩
select char('SQ') as series,c.branch,substr(a.station_a,3,2) as aracde,bm_cert as chdrnum,kinds as cnttype,premium as acctamt,premium as acctamt_std,
char(jf_way) as freq,smallint(jfyears) as period,app_flag as statcode,a.agency_code as agntnum,d.name,
administrator.date_int(a.make_time) as trandate,'y' as sort,e.agency_group as teamnum,e.group_name as teamname,'' as statcode2
        from administrator.rta19 a,administrator.hn_branch c,administrator.hn_agency d,administrator.hn_agency_group e
                where  a.station = c.station and a.agency_code=d.agency_code  and d.agency_group = e.agency_group
                and app_flag IN ('1','WT')  and make_time=DATE(current timestamp)
                and a.agency_code not in (select agency_code from administrator.agency where agency_group
                IN ('08900069','08900070','08900071')) and a.agency_code not like '808%' and
                a.agency_code not like '%A' and a.agency_code not like '%WY%'
union all
select char('SQ') as series,c.branch,substr(a.station_a,3,2) as aracde,bm_cert as chdrnum,kinds as cnttype,-premium as acctamt,-premium as acctamt_std,
char(jf_way) as freq,smallint(jfyears) as period,app_flag as statcode,a.agency_code as agntnum,d.name,
administrator.date_int(a.rq_make) as trandate,'y' as sort,e.agency_group as teamnum,e.group_name as teamname,'' as statcode2
        from administrator.rta1b9 a,administrator.hn_branch c,administrator.hn_agency d,administrator.hn_agency_group e
                where  a.station = c.station and a.agency_code=d.agency_code  and d.agency_group = e.agency_group
                and pg_type in ('WT','BT')  and a.rq_make=DATE(current timestamp)
                and a.agency_code not in (select agency_code from administrator.agency where agency_group
                IN ('08900069','08900070','08900071')) and a.agency_code not like '808%' and
                a.agency_code not like '%A' and a.agency_code not like '%WY%';
end @


