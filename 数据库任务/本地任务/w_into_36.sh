#!/bin/sh
db2 "with mytable as(
select 
        company_name,
        row_number() over() as rank_m,
        sum(case when series='个险' then bf end)/10000 as gx,
        sum(case when series='银保' then bf end)/10000 as yb,
        sum(case when series='电销' then bf end)/10000 as dx,
        sum(case when series='经代' then bf end)/10000 as jd,
        sum(bf)/10000 as all
        from administrator.hn_all_dgx                     
                where company in(select 
                                        company
                                        from administrator.hn_all_dgx                                 
                                                group by company
                                                        order by sum(bf) desc fetch first 5 rows only)
                and trandate between decimal(current date - day(current date -1 days) days) and decimal(current date -1 days)
                        group by company_name
                                order by all desc),
hn as(
select 
        sum(bf)/10000 as hnbf
        from administrator.hn_all_dgx
                where company='D'
                and trandate between decimal(current date - day(current date -1 days) days) and decimal(current date -1 days)                
        )
select 
        company_name,
        rank_m,
        all,
        hnbf-all as diff,
        gx,
        yb,
        value(dx,0),
        value(jd,0)
                from mytable a,hn b                                                  
   
                                ">dgx
sed -n '4,8p' dgx>dgx.bak
mv dgx.bak dgx
db2 connect reset
scp dgx db2inst@10.19.19.36:/home/db2inst/ && rm dgx
