create  procedure p.get_ybssyj()
begin atomic
--删除当天数据
delete from administrator.hn_ybyj where trandate =  year(current date)*10000+month(current date)*100+day(current date);
insert into administrator.hn_ybyj
--拓展当天承保数据
select char('TZ') as series,branch,aracde,chdrnum,cnttype,acctamt,acctamt_std,'' as freq,0 as period,batc_type as statcode,
agntnum,agntname as name,trandate,'g' as sort,teamnum,teamname,'ac' as statcode2
        from administrator.ipe_acct_rt
                where agtype IN ('BY','BZ') and trandate = year(date(current timestamp))*10000+month(date(current timestamp))*100
                +day(date(current timestamp))
union all
--信贷无忧区分--拓展
select char('TZ') as series,a.branch,a.aracde,a.chdrnum,a.cnttype,acctamt,acctamt_std,'' as freq,0 as period,a.batc_type as statcode,
a.agntnum as agntnum,substr(a.agntname,1,9) as agntname,a.trandate,'g' as sort,'' as teamnum,'' as teamname,'ac' as statcode2
        from administrator.ipe_acct_rt a
                where agtype IN ('BA') and srcebus='BY'
                and trandate = year(current date)*10000+month(current date)*100+day(current date) 
union all
--信贷无忧区分--首期
select char('SQ') as series,a.branch,a.aracde,a.chdrnum,a.cnttype,acctamt,acctamt_std,'' as freq,0 as period,a.batc_type as statcode,
a.agntnum as agntnum,substr(a.agntname,1,9) as agntname,a.trandate,'g' as sort,'' as teamnum,'' as teamname,'ac' as statcode2
        from administrator.ipe_acct_rt a
                where agtype IN ('BA') and srcebus<>'BY'
                and trandate = year(current date)*10000+month(current date)*100+day(current date) 
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
--首期银保业绩
select char('SQ') as series,c.branch,substr(a.station_a,3,2) as aracde,bm_cert as chdrnum,kinds as cnttype,premium as acctamt,premium as acctamt_std,
char(jf_way) as freq,smallint(jfyears) as period,app_flag as statcode,a.agency_code as agntnum,d.name,
administrator.date_int(a.make_time) as trandate,'y' as sort,e.agency_group as teamnum,e.group_name as teamname,'' as statcode2
        from administrator.rta19 a,administrator.hn_branch c,administrator.hn_agency d,administrator.hn_agency_group e
                where  a.station = c.station and a.agency_code=d.agency_code  and d.agency_group = e.agency_group
                and app_flag IN ('1','WT')  and make_time=DATE(current timestamp)
                and a.agency_code not like '808%' and
                a.agency_code not like '%A' and a.agency_code not like '%WY%'
union all
select char('SQ') as series,c.branch,substr(a.station_a,3,2) as aracde,bm_cert as chdrnum,kinds as cnttype,-premium as acctamt,-premium as acctamt_std,
char(jf_way) as freq,smallint(jfyears) as period,app_flag as statcode,a.agency_code as agntnum,d.name,
administrator.date_int(a.rq_make) as trandate,'y' as sort,e.agency_group as teamnum,e.group_name as teamname,'' as statcode2
        from administrator.rta1b9 a,administrator.hn_branch c,administrator.hn_agency d,administrator.hn_agency_group e
                where  a.station = c.station and a.agency_code=d.agency_code  and d.agency_group = e.agency_group
                and pg_type in ('WT','BT')  and a.rq_make=DATE(current timestamp)
                and a.agency_code not like '808%' and
                a.agency_code not like '%A' and a.agency_code not like '%WY%';
end @


