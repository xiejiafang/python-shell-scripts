create  procedure p.weixin_dgx(dateStart decimal(8,0),dateEnd decimal(8,0),sys varchar(3),index_num char(3),information varchar(50))
begin atomic
        declare info varchar(1000) default '';
        declare info_temp varchar(50) default '';
        set info = information||CHR(10)||'截止时间:'||trim(char(day(current timestamp)))||'日'
        ||trim(char(hour(current timestamp)))||'点'||trim(char(minute(current timestamp)))||'分'
        ||CHR(10)||'============='||CHR(10);
        for row as
                with mytable as
                (select value(branch,'ALL') as branch,value(name,'合计') as name,
                case when sum(bf)>0 then STRIP(CHAR(dec(sum(bf)/10000,10,2)),l,'0') when sum(bf)=0 then '0'
                else '-'||strip(STRIP(CHAR(dec(sum(bf)/10000,10,2)),b,'-'),l,'0') end as bf,sum(bf) as bbbf
                        from
                        (select * 
                                from table(f.get_yb_dgx_zs(dateStart,dateEnd,sys))a
                        union all
                        select 'FIC' as branch,'FIC' as name,sum(bf) as bf,'R' as group 
                                from table(f.get_yb_dgx_zs(dateStart,dateEnd,'FIC'))b )a
                                        group by grouping sets((branch,name),()) order by bbbf desc)
                select '['||trim(name) || ']' || bf ||CHR(10) as result from mytable
                do
                set info_temp = result;
                set info = info || info_temp;
        end for;
        update administrator.hn_weixin set index_res=info where index_name = index_num;
end @
