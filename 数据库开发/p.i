create  procedure p.get_yb_agent()
begin
        declare c1 cursor with return for
        select case when (agency_code like '808%' or  agency_code like '%A' or group_name like '%XQ%') then 'TZ'
        when fic ='Y' then 'FIC' else 'SQ' end as series,a.name,a.agency_code,a.branch,a.branchname,a.agency_group,a.group_name,a.fic 
        from 
        (select a.name,a.agency_code,c.branch,c.name as branchname,a.agency_group,b.group_name,
        case when (select distinct agntnum from administrator.hn_agntinfo d 
                where a.id_nu=d.secuityno and branch='10' and agtype in('BC','BD','BM','BP','BY','BZ')fetch first 1 rows only) is null then 'N'
        else 'Y' end as fic   
                from administrator.hn_agency a,administrator.hn_agency_group b,administrator.hn_branch c 
                        where a.agency_group=b.agency_group and a.station=c.station and a.agency_code not like '%WY%' and 
                        a.state in('0','10','11'))a;
        open c1;
end @    
