db2 connect to hnii
db2 "
export to year.txt of del
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
where trandate between 20170101 and 20171231
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
