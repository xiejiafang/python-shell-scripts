create procedure p.get_ybyj()
begin atomic
        declare dateStart decimal(8,0);
        declare dateEnd decimal(8,0);
        declare f_dateStart date;
        declare f_dateEnd date;
        set dateStart = year(date(current timestamp) - 2
years)*10000+0101;
        set dateEnd = year(date(current
timestamp))*10000+month(date(current timestamp))*100+day(date(current
timestamp));
        set f_dateStart = trim(char(year(date(current timestamp) - 2
years))) || '-01-01';
        set f_dateEnd = date(current timestamp);

--拓展个险业绩（用职级界定）
insert into administrator.hn_ybyj_temp
select char('TZ') as
series,a.branch,a.aracde,a.chdrnum,a.cnttype,acctamt,acctamt_std,freq,smallint(period)
as period,b.statcode,
a.agntnum,a.agntname as name,a.trandate,'g' as
sort,a.teamnum,a.teamname,'' as statcode2
        from administrator.hn_acctinfo a,administrator.hn_chdrinfo b
                where a.chdrnum = b.chdrnum and agtype IN ('BY','BZ')
and a.trandate between dateStart and dateEnd;
insert into administrator.hn_ybyj_temp
--拓展个险业绩，因这部分业绩均用信贷无忧手工单，故关连zcdrpf用保单号区分
select char('TZ') as
series,a.branch,a.aracde,a.chdrnum,a.cnttype,acctamt,acctamt_std,freq,smallint(period)
as period,b.statcode,
c.reportag01 as agntnum,d.name,a.trandate,'g' as sort,e.agency_group as
teamnum,e.group_name as teamname,'' as statcode2
        from administrator.hn_acctinfo a,administrator.hn_chdrinfo
b,administrator.hn_zcdrpf c,administrator.hn_agency
d,administrator.hn_agency_group e
                where a.chdrnum = b.chdrnum and a.chdrnum = c.chdrnum
and c.reportag01=d.agency_code
                and d.agency_group = e.agency_group and (c.reportag01
like
'808%' or  c.reportag01 like '%A') and agtype IN ('BA','FM')
                and a.trandate between dateStart and dateEnd;
insert into administrator.hn_ybyj_temp
--拓展银保业绩
select char('TZ') as series,c.branch,medi_code as aracde,bm_cert as
chdrnum,kinds as cnttype,jf_je as acctamt,jf_je as acctamt_std,
char(jf_way) as freq,smallint(jfyears) as period,app_flag as
statcode,a.agency_code as agntnum,d.name,
administrator.date_int(a.jf_rq) as trandate,'y' as sort,e.agency_group
as teamnum,e.group_name as teamname,a.detail_flag as statcode2
        from administrator.hn_basdata a,administrator.hn_branch
c,administrator.hn_agency d,administrator.hn_agency_group e
                where (a.agency_code like '808%' or  a.agency_code like
'%A') and a.station = c.station
                and a.agency_code=d.agency_code  and d.agency_group =
e.agency_group
                and app_flag IN ('1','WT')  and a.jf_rq between
f_dateStart and f_dateEnd ;
--FIC个险业绩（用职级界定）
insert into administrator.hn_ybyj_temp
select char('FIC') as
series,a.branch,a.aracde,a.chdrnum,a.cnttype,acctamt,acctamt_std,freq,smallint(period)
as period,b.statcode,
a.agntnum,a.agntname as name,a.trandate,'g' as
sort,a.teamnum,a.teamname,'' as statcode2
        from administrator.hn_acctinfo a,administrator.hn_chdrinfo b
                where a.chdrnum = b.chdrnum and agtype IN
('BC','BD','BM','BP') and a.trandate between dateStart and dateEnd;
insert into administrator.hn_ybyj_temp
--FIC个险业绩，因这部分业绩均用信贷无忧手工单，故关连zcdrpf用保单号区分
select char('FIC') as
series,a.branch,a.aracde,a.chdrnum,a.cnttype,acctamt,acctamt_std,freq,smallint(period)
as period,b.statcode,
c.reportag01 as agntnum,d.name,a.trandate,'g' as sort,e.agency_group as
teamnum,e.group_name as teamname,'' as statcode2
        from administrator.hn_acctinfo a,administrator.hn_chdrinfo
b,administrator.hn_zcdrpf c,administrator.hn_agency
d,administrator.hn_agency_group e
                where a.chdrnum = b.chdrnum and a.chdrnum = c.chdrnum
and c.reportag01=d.agency_code
                and d.agency_group = e.agency_group and c.reportag01 in
                        (select agency_code from administrator.hn_agency
where agency_group
                                IN
('08900069','08900070','08900071','08900074'))  and agtype IN
('BA','FM')
                and a.trandate between dateStart and dateEnd;
insert into administrator.hn_ybyj_temp
--FIC银保业绩
select char('FIC') as series,c.branch,medi_code as aracde,bm_cert as
chdrnum,kinds as cnttype,jf_je as acctamt,jf_je as acctamt_std,
char(jf_way) as freq,smallint(jfyears) as period,app_flag as
statcode,a.agency_code as agntnum,d.name,
administrator.date_int(a.jf_rq) as trandate,'y' as sort,e.agency_group
as teamnum,e.group_name as teamname,a.detail_flag as statcode2
        from administrator.hn_basdata a,administrator.hn_branch
c,administrator.hn_agency d,administrator.hn_agency_group e
                where a.agency_code in (select agency_code from
administrator.hn_agency where agency_group
                        IN
('08900069','08900070','08900071','08900074')) and a.station = c.station
and a.agency_code=d.agency_code
                and d.agency_group = e.agency_group and app_flag IN
('1','WT')  and a.jf_rq between f_dateStart and f_dateEnd ;
insert into administrator.hn_ybyj_temp
--首期个险业绩
select char('SQ') as
series,a.branch,a.aracde,a.chdrnum,a.cnttype,acctamt,acctamt_std,freq,smallint(period)
as period,b.statcode,
c.reportag01 as agntnum,d.name,a.trandate,'g' as sort,e.agency_group as
teamnum,e.group_name as teamname,'' as statcode2
        from administrator.hn_acctinfo a,administrator.hn_chdrinfo
b,administrator.hn_zcdrpf c,administrator.hn_agency
d,administrator.hn_agency_group e
                where a.chdrnum = b.chdrnum and a.chdrnum = c.chdrnum
and c.reportag01=d.agency_code
                and d.agency_group = e.agency_group and c.reportag01 not
in
                        (select agency_code from administrator.hn_agency
where agency_group
                        IN
('08900069','08900070','08900071','08900074')) and c.reportag01 not like
'808%'
                and c.reportag01 not like '%A'  and agtype IN
('BA','FM') and a.trandate between dateStart and dateEnd;
insert into administrator.hn_ybyj_temp
--首期银保业绩，不包括续期
select char('SQ') as series,c.branch,medi_code as aracde,bm_cert as
chdrnum,kinds as cnttype,jf_je as acctamt,jf_je as acctamt_std,
char(jf_way) as freq,smallint(jfyears) as period,app_flag as
statcode,a.agency_code as agntnum,d.name,
administrator.date_int(a.jf_rq) as trandate,'y' as sort,e.agency_group
as teamnum,e.group_name as teamname,a.detail_flag as statcode2
        from administrator.hn_basdata a,administrator.hn_branch
c,administrator.hn_agency d,administrator.hn_agency_group e
                where a.agency_code not in (select agency_code from
administrator.hn_agency where agency_group
                        IN
('08900069','08900070','08900071','08900074')) and a.agency_code not
like '808%'
                and a.agency_code not like '%A' and a.agency_code not
like '%WY%' and a.station = c.station
                and a.agency_code=d.agency_code
                and d.agency_group = e.agency_group and app_flag IN
('1','WT')  and a.jf_rq between f_dateStart and f_dateEnd ;
end @

