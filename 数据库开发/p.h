create procedure p.bj_data(yyyymm decimal(6,0))
begin
        declare MonStart decimal(8,0);
        declare MonEnd decimal(8,0);
        declare t_mon decimal(6,0);
        SET MonStart = yyyymm*100+1;
        SET MonEnd = yyyymm*100+31;
        Set t_mon = yyyymm;
        -- cl
        delete from administrator.bj_stat where dl_ct = 'CL';
        insert into administrator.bj_stat(branch,ywxl,fl_code,dl_ct,js,acct,trandate)
        select station,kinds,b.fl_code,'CL',value(js,0),value(bf/10000*1.00,0),t_mon
                from table(f.get_gx_cnttype(19960101,20130731))as a ,
                administrator.bj_cnttype_fl b
                        where a.kinds=b.crtable and station is not null
        union all
        select 'jy',kinds,b.fl_code,'CL',value(js,0),value(bf/10000*1.00,0),t_mon
        from table(f.get_gx_cnttype_jy(19960101,20130731))as a ,
                administrator.bj_cnttype_fl b
                        where a.kinds=b.crtable and station is not null
        union all
        select c.branch,kinds,b.fl_code,'CL',value(js,0),value(bf/10000*1.00,0),t_mon
                from table(f.get_yb_cnttype(19960101,20130731))as a ,
                administrator.bj_cnttype_fl b,
                administrator.hn_branch c
                        where a.kinds=b.crtable
                        and a.station=c.station
                        and c.branch is not null;
        -- dc
        delete from administrator.bj_stat where dl_ct = 'DC';
        insert into administrator.bj_stat(branch,ywxl,fl_code,dl_ct,js,acct,trandate)
        select station,kinds,b.fl_code,'DC',value(js,0),value(bf/10000*1.00,0),t_mon
                from table(f.get_gx_cnttype(MonStart,MonEnd))as a ,
                administrator.bj_cnttype_fl b
                        where a.kinds=b.crtable and station is not null
        union all
        select 'jy',kinds,b.fl_code,'DC',value(js,0),value(bf/10000*1.00,0),t_mon
        from table(f.get_gx_cnttype_jy(MonStart,MonEnd))as a ,
                administrator.bj_cnttype_fl b
                        where a.kinds=b.crtable and station is not null
        union all
        select c.branch,kinds,b.fl_code,'DC',value(js,0),value(bf/10000*1.00,0),t_mon
                from table(f.get_yb_cnttype(MonStart,MonEnd))as a ,
                administrator.bj_cnttype_fl b,
                administrator.hn_branch c
                        where a.kinds=b.crtable
                        and a.station=c.station
                        and c.branch is not null;
        -- lc
        delete from administrator.bj_stat where dl_ct = 'LC';
        insert into administrator.bj_stat(branch,ywxl,fl_code,dl_ct,js,acct,trandate)
        select station,kinds,b.fl_code,'LC',value(js,0),value(bf/10000*1.00,0),t_mon
                from table(f.get_gx_cnttype(20140101,MonEnd))as a ,
                administrator.bj_cnttype_fl b
                        where a.kinds=b.crtable and station is not null
        union all
        select 'jy',kinds,b.fl_code,'LC',value(js,0),value(bf/10000*1.00,0),t_mon
        from table(f.get_gx_cnttype_jy(20140101,MonEnd))as a ,
                administrator.bj_cnttype_fl b
                        where a.kinds=b.crtable and station is not null
        union all
        select c.branch,kinds,b.fl_code,'LC',value(js,0),value(bf/10000*1.00,0),t_mon
                from table(f.get_yb_cnttype(20140101,MonEnd))as a ,
                administrator.bj_cnttype_fl b,
                administrator.hn_branch c
                        where a.kinds=b.crtable
                        and a.station=c.station
                        and c.branch is not null;
end@
