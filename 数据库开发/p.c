create procedure p.weixin_gxhd(monNumber decimal(6,0),mysort varchar(3),hdrl varchar(8),allrl varchar(8),
index_num char(3),information varchar(50))
begin atomic
        declare info varchar(1000) default '';
        declare info_temp varchar(50) default '';
        set info = information||CHR(10)||'截止时间:'||trim(char(day(current timestamp)))||'日'
        ||trim(char(hour(current timestamp)))||'点'||trim(char(minute(current timestamp)))||'分'
        ||CHR(10)||'============='||CHR(10);
        for row as
                with rate as(
                select value(b.name,'分公司平均值') as branch,
                dec(sum(case when kpi_name = hdrl then thisyear end)*1.0000/sum(case when kpi_name = allrl then thisyear end)*100,10,2) as rate
                        from administrator.hn_gxkpi a,administrator.hn_branch b
                                where a.branch=b.branch and sort = mysort and kpi_name in(hdrl,allrl) and month=monNumber
                                        group by grouping sets(b.name,()) order by rate desc)
                select '['||trim(branch)||']'||strip(trim(char(rate)),l,'0')||'%'||CHR(10) as result
                        from rate     
                do
                set info_temp = result;
                set info = info || info_temp;
        end for;
        update administrator.hn_weixin set index_res=info where index_name = index_num;
end @
