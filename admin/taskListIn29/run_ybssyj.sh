#!/bin/sh
#引入用户环境变量
echo `date`;
source /home/db2inst/.bash_profile
db2 "call p.get_ybssyj()";
db2 "update administrator.hn_ybyj set statcode = case when acctamt > 0 then 'CB' else 'CD' end
        where trandate = year(date(current timestamp))*10000+month(date(current timestamp))*100
        +day(date(current timestamp)) and cnttype = '523'   ";
#db2 "update administrator.hn_ybyj a set series=(select fl from hn_agency2014 b where a.agntnum=b.agency_code)";
#db2 "update administrator.hn_ybyj set series ='TZ' where series='XQ'";
#db2 "update administrator.hn_ybyj set series ='FIC' where series='FC'";    
db2 connect reset;
exit;
