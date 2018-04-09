begin atomic
declare today_date DATE;
declare yester_date date;
declare tran_date decimal(8,0);
DECLARE tran_date_end decimal(8,0);

set today_date=DATE(current timestamp);
set yester_date=(today_date-bef_day days);
set tran_date =YEAR ( yester_date)*10000+MONTH( yester_date)*100+DAY ( yester_date);
--set tran_date=20060101;
set tran_date_end=99999999;

delete from hn_acctinfo where  trandate between tran_date and tran_date_end;

insert into hn_acctinfo
select CHDRNUM,INS,FREQ,INSS,PERIOD,ACCTAMT,ACCTAMT_STD,SUMINS,TRANDATE,BATCTRCDE,
BATC_TYPE,BATC_TYPE_C,LT_IND,HC_IND,AGNTNUM,AGNTNAME,AGTYPE,TEAMNUM,TEAMNAME,
TEAMTYPE,PARTNUM,PARTNAME,PARTTYPE,ARACDE,BRANCH,SACSCODE,SACSTYP,CNTTYPE,HPRRCVDT
,acctamt*(select cmrate from zfeepf9 b where  a.trandate between currfrom and currto and itemtabl='TZ813'
and  a.inss=b.crtable and a.period > b.zrterm01 and  a.period <= b.zrterm02 order by tyearno
fetch first 1 rows only) /100,val_rate,acctamt_std_a,vargraphic(srcebus),srcebus
from v_acct_day9 a
where a.trandate between tran_date and tran_date_end and a.inss not in ('YLAP','YLBP','U02P','U05P','U07P','U10P');

insert into hn_acctinfo
select CHDRNUM,INS,FREQ,INSS,PERIOD,ACCTAMT,ACCTAMT_STD,SUMINS,TRANDATE,BATCTRCDE,
BATC_TYPE,BATC_TYPE_C,LT_IND,HC_IND,AGNTNUM,AGNTNAME,AGTYPE,TEAMNUM,TEAMNAME,
TEAMTYPE,PARTNUM,PARTNAME,PARTTYPE,ARACDE,BRANCH,SACSCODE,SACSTYP,CNTTYPE,HPRRCVDT
,acctamt*(select cmrate from zfeepf9 b where  a.trandate between currfrom and currto and itemtabl='TZ813'
and  b.crtable='YL10' and a.period > b.zrterm01 and  a.period <= b.zrterm02 order by tyearno
fetch first 1 rows only) /100,val_rate,acctamt_std_a,vargraphic(srcebus),srcebus
from v_acct_day9 a
where a.trandate between tran_date and tran_date_end and a.inss  in ('YLAP','YLBP') and period<=10 ;

insert into hn_acctinfo
select CHDRNUM,INS,FREQ,INSS,PERIOD,ACCTAMT,ACCTAMT_STD,SUMINS,TRANDATE,BATCTRCDE,
BATC_TYPE,BATC_TYPE_C,LT_IND,HC_IND,AGNTNUM,AGNTNAME,AGTYPE,TEAMNUM,TEAMNAME,
TEAMTYPE,PARTNUM,PARTNAME,PARTTYPE,ARACDE,BRANCH,SACSCODE,SACSTYP,CNTTYPE,HPRRCVDT
,acctamt*(select cmrate from zfeepf9 b where  a.trandate between currfrom and currto and itemtabl='TZ813'
and  b.crtable='YL15' and a.period > b.zrterm01 and  a.period <= b.zrterm02 order by tyearno
fetch first 1 rows only) /100,val_rate,acctamt_std_a,vargraphic(srcebus),srcebus
from v_acct_day9 a
where a.trandate between tran_date and tran_date_end and a.inss  in ('YLAP','YLBP') and period>10 ;

insert into hn_acctinfo
select CHDRNUM,INS,FREQ,INSS,PERIOD,ACCTAMT,ACCTAMT_STD,SUMINS,TRANDATE,BATCTRCDE,
BATC_TYPE,BATC_TYPE_C,LT_IND,HC_IND,AGNTNUM,AGNTNAME,AGTYPE,TEAMNUM,TEAMNAME,
TEAMTYPE,PARTNUM,PARTNAME,PARTTYPE,ARACDE,BRANCH,SACSCODE,SACSTYP,CNTTYPE,HPRRCVDT
,acctamt*(select cmrate from zfeepf9 b where  a.trandate between currfrom and currto and itemtabl='TZ813'
and  b.crtable='U02A' and a.period > b.zrterm01 and  a.period <= b.zrterm02 order by tyearno
fetch first 1 rows only)/100,val_rate,acctamt_std_a,vargraphic(srcebus),srcebus
from v_acct_day9 a
where a.trandate between tran_date and tran_date_end and a.inss='U02P' and acctamt=acctamt
and acctamt<=5000 and acctamt>=-5000 ;

insert into hn_acctinfo
select CHDRNUM,INS,FREQ,INSS,PERIOD,ACCTAMT,ACCTAMT_STD,SUMINS,TRANDATE,BATCTRCDE,
BATC_TYPE,BATC_TYPE_C,LT_IND,HC_IND,AGNTNUM,AGNTNAME,AGTYPE,TEAMNUM,TEAMNAME,
TEAMTYPE,PARTNUM,PARTNAME,PARTTYPE,ARACDE,BRANCH,SACSCODE,SACSTYP,CNTTYPE,HPRRCVDT
,5000*(select cmrate from zfeepf9 b where  a.trandate between currfrom and currto and itemtabl='TZ813'
and  b.crtable='U02A' and a.period > b.zrterm01 and  a.period <= b.zrterm02 order by tyearno
fetch first 1 rows only)/100+
(acctamt-5000)*(select cmrate from zfeepf9 b where  a.trandate between currfrom and currto and itemtabl='TZ813'
and  b.crtable='U02B' and a.period > b.zrterm01 and  a.period <= b.zrterm02 order by tyearno
fetch first 1 rows only)/100,val_rate,acctamt_std_a,vargraphic(srcebus),srcebus
from v_acct_day9 a
where a.trandate between tran_date and tran_date_end and a.inss='U02P' and acctamt=acctamt and acctamt>5000;

insert into hn_acctinfo
select CHDRNUM,INS,FREQ,INSS,PERIOD,ACCTAMT,ACCTAMT_STD,SUMINS,TRANDATE,BATCTRCDE,
BATC_TYPE,BATC_TYPE_C,LT_IND,HC_IND,AGNTNUM,AGNTNAME,AGTYPE,TEAMNUM,TEAMNAME,
TEAMTYPE,PARTNUM,PARTNAME,PARTTYPE,ARACDE,BRANCH,SACSCODE,SACSTYP,CNTTYPE,HPRRCVDT
,-5000*(select cmrate from zfeepf9 b where  a.trandate between currfrom and currto and itemtabl='TZ813'
and  b.crtable='U02A' and a.period > b.zrterm01 and  a.period <= b.zrterm02 order by tyearno
fetch first 1 rows only)/100+
(acctamt+5000)*(select cmrate from zfeepf9 b where  a.trandate between currfrom and currto and itemtabl='TZ813'
and  b.crtable='U02B' and a.period > b.zrterm01 and  a.period <= b.zrterm02 order by tyearno
fetch first 1 rows only)/100,val_rate,acctamt_std_a,vargraphic(srcebus),srcebus
from v_acct_day9 a
where a.trandate between tran_date and tran_date_end and a.inss='U02P' and acctamt=acctamt and acctamt<-5000;

insert into hn_acctinfo
select CHDRNUM,INS,FREQ,INSS,PERIOD,ACCTAMT,ACCTAMT_STD,SUMINS,TRANDATE,BATCTRCDE,
BATC_TYPE,BATC_TYPE_C,LT_IND,HC_IND,AGNTNUM,AGNTNAME,AGTYPE,TEAMNUM,TEAMNAME,
TEAMTYPE,PARTNUM,PARTNAME,PARTTYPE,ARACDE,BRANCH,SACSCODE,SACSTYP,CNTTYPE,HPRRCVDT
,acctamt*(select cmrate from zfeepf9 b where  a.trandate between currfrom and currto and itemtabl='TZ813'
and  b.crtable='U05A' and a.period > b.zrterm01 and  a.period <= b.zrterm02 order by tyearno
fetch first 1 rows only)/100,val_rate,acctamt_std_a,vargraphic(srcebus),srcebus
from v_acct_day9 a
where a.trandate between tran_date and tran_date_end and a.inss='U05P' and acctamt=acctamt_STD;

insert into hn_acctinfo
select CHDRNUM,INS,FREQ,INSS,PERIOD,ACCTAMT,ACCTAMT_STD,SUMINS,TRANDATE,BATCTRCDE,
BATC_TYPE,BATC_TYPE_C,LT_IND,HC_IND,AGNTNUM,AGNTNAME,AGTYPE,TEAMNUM,TEAMNAME,
TEAMTYPE,PARTNUM,PARTNAME,PARTTYPE,ARACDE,BRANCH,SACSCODE,SACSTYP,CNTTYPE,HPRRCVDT
,acctamt*(select cmrate from zfeepf9 b where  a.trandate between currfrom and currto and itemtabl='TZ813'
and  b.crtable='U05P' and a.period > b.zrterm01 and  a.period <= b.zrterm02 order by tyearno
fetch first 1 rows only)/100,val_rate,acctamt_std_a,vargraphic(srcebus),srcebus
from v_acct_day9 a
where a.trandate between tran_date and tran_date_end and a.inss='U05P' and acctamt<>acctamt_STD;


insert into hn_acctinfo
select CHDRNUM,INS,FREQ,INSS,PERIOD,ACCTAMT,ACCTAMT_STD,SUMINS,TRANDATE,BATCTRCDE,
BATC_TYPE,BATC_TYPE_C,LT_IND,HC_IND,AGNTNUM,AGNTNAME,AGTYPE,TEAMNUM,TEAMNAME,
TEAMTYPE,PARTNUM,PARTNAME,PARTTYPE,ARACDE,BRANCH,SACSCODE,SACSTYP,CNTTYPE,HPRRCVDT
,acctamt*(select cmrate from zfeepf9 b where  a.trandate between currfrom and currto and itemtabl='TZ813'
and  b.crtable='U07A' and a.period > b.zrterm01 and  a.period <= b.zrterm02 order by tyearno
fetch first 1 rows only)/100,val_rate,acctamt_std_a,vargraphic(srcebus),srcebus
from v_acct_day9 a
where a.trandate between tran_date and tran_date_end and a.inss='U07P' and acctamt=acctamt_STD;

insert into hn_acctinfo
select CHDRNUM,INS,FREQ,INSS,PERIOD,ACCTAMT,ACCTAMT_STD,SUMINS,TRANDATE,BATCTRCDE,
BATC_TYPE,BATC_TYPE_C,LT_IND,HC_IND,AGNTNUM,AGNTNAME,AGTYPE,TEAMNUM,TEAMNAME,
TEAMTYPE,PARTNUM,PARTNAME,PARTTYPE,ARACDE,BRANCH,SACSCODE,SACSTYP,CNTTYPE,HPRRCVDT
,acctamt*(select cmrate from zfeepf9 b where  a.trandate between currfrom and currto and itemtabl='TZ813'
and  b.crtable='U07P' and a.period > b.zrterm01 and  a.period <= b.zrterm02 order by tyearno
fetch first 1 rows only)/100,val_rate,acctamt_std_a,vargraphic(srcebus),srcebus
from v_acct_day9 a
where a.trandate between tran_date and tran_date_end and a.inss='U07P' and acctamt<>acctamt_STD;


insert into hn_acctinfo
select CHDRNUM,INS,FREQ,INSS,PERIOD,ACCTAMT,ACCTAMT_STD,SUMINS,TRANDATE,BATCTRCDE,
BATC_TYPE,BATC_TYPE_C,LT_IND,HC_IND,AGNTNUM,AGNTNAME,AGTYPE,TEAMNUM,TEAMNAME,
TEAMTYPE,PARTNUM,PARTNAME,PARTTYPE,ARACDE,BRANCH,SACSCODE,SACSTYP,CNTTYPE,HPRRCVDT
,acctamt*(select cmrate from zfeepf9 b where  a.trandate between currfrom and currto and itemtabl='TZ813'
and  b.crtable='U10A' and a.period > b.zrterm01 and  a.period <= b.zrterm02 order by tyearno
fetch first 1 rows only)/100,val_rate,acctamt_std_a,vargraphic(srcebus),srcebus
from v_acct_day9 a
where a.trandate between tran_date and tran_date_end and a.inss='U10P' and acctamt=acctamt_STD;

insert into hn_acctinfo
select CHDRNUM,INS,FREQ,INSS,PERIOD,ACCTAMT,ACCTAMT_STD,SUMINS,TRANDATE,BATCTRCDE,
BATC_TYPE,BATC_TYPE_C,LT_IND,HC_IND,AGNTNUM,AGNTNAME,AGTYPE,TEAMNUM,TEAMNAME,
TEAMTYPE,PARTNUM,PARTNAME,PARTTYPE,ARACDE,BRANCH,SACSCODE,SACSTYP,CNTTYPE,HPRRCVDT
,acctamt*(select cmrate from zfeepf9 b where  a.trandate between currfrom and currto and itemtabl='TZ813'
and  b.crtable='U10P' and a.period > b.zrterm01 and  a.period <= b.zrterm02 order by tyearno
fetch first 1 rows only)/100,val_rate,acctamt_std_a,vargraphic(srcebus),srcebus
from v_acct_day9 a
where a.trandate between tran_date and tran_date_end and a.inss='U10P' and acctamt<>acctamt_STD;

--update administrator.hn_acctinfo
--set (aracde,teamnum,teamname,partnum,partname,agtype)=
--(select aracde,teamnum,teamname,partnum,partname,agtype from hn_agnt_his
--where agntnum=administrator.hn_acctinfo.agntnum and mon=int(administrator.hn_acctinfo.trandate/100))
--where trandate between tran_date and tran_date_end ;



update hn_acctinfo a
set (agntnum,agntname,aracde)=(select distinct  zcllctor,zcllname,zcllara  from hn_xuqidetail
where a.chdrnum=hn_xuqidetail.chdrnum  and zcllctor<>'')
WHERE trandate between tran_date and tran_date_end and batctrcde='T679'  AND
chdrnum in
(select distinct  a.chdrnum from hn_acctinfo a ,hn_xuqidetail b
 where a.trandate between tran_date and tran_date_end and a.chdrnum=b.chdrnum and zcllctor<>'');
 
 update hn_acctinfo a
set teamnum='',teamname='',partnum='',partname='',agtype='RC'
WHERE trandate between tran_date and tran_date_end and batctrcde='T679'  AND
chdrnum in
(select distinct  a.chdrnum from hn_acctinfo a ,hn_xuqidetail b
 where a.trandate between tran_date and tran_date_end and a.chdrnum=b.chdrnum and zcllctor<>'');


insert into hn_acctinfo (CHDRNUM,INS,FREQ,INSS,PERIOD,ACCTAMT,ACCTAMT_STD,SUMINS,TRANDATE,BATCTRCDE,BATC_TYPE,BATC_TYPE_C,LT_IND,HC_IND,AGNTNUM,AGNTNAME,AGTYPE,TEAMNUM,TEAMNAME,tEAMTYPE,PARTNUM,PARTNAME,PARTTYPE,ARACDE,BRANCH,SACSCODE,SACSTYP,CNTTYPE,HPRRCVDT,fyc,acctamt_std_a)
select
'G'||substr(a.GRPPOLNO,5,7),a.RISKCODE,'C',a.RISKCODE||'C',1,a.ACCTAMT,a.ACCTAMT,0,a.TRANDATE,'TGJC','CB',a.BATC_TYPE,'C','C',a.AGNTNUM,
b.AGNTNAME,b.AGTYPE,b.TEAMNUM,b.TEAMNAME,b.TEAMTYPE,b.PARTNUM,b.PARTNAME,b.PARTTYPE,b.ARACDE,b.BRANCH,'LE','LP',a.RISKCODE,a.TRANDATE,0,ACCTAMT
 from bi_ipe_eba_crsab9 a
left join hn_agnt_his b on a.agntnum=b.agntnum
where a.trandate between tran_date and tran_date_end
and b.mon=int(a.trandate/100)  with ur;

--保单生存金释放删除，计入保费20120401之后才有
delete from hn_acctinfo where  trandate between tran_date and tran_date_end and batctrcde='BA67';


--更新空的aracde信息
update administrator.hn_acctinfo
set (aracde,teamnum,teamname,partnum,partname,agtype)=
(select aracde,teamnum,teamname,partnum,partname,agtype from administrator.hn_agntinfo
where agntnum=administrator.hn_acctinfo.agntnum
)
where trandate between tran_date and tran_date_end and aracde is null ;

--更新健康人生FYC

update administrator.hn_acctinfo
set fyc=(case when period=5 and lt_ind='Y' then acctamt_std*0.15
when period=10 and lt_ind='Y' then acctamt_std*0.30
when period=15 and lt_ind='Y' then acctamt_std*0.35
when period=20 and lt_ind='Y' then acctamt_std*0.40
when period not in(5,10,15,20) and lt_ind='Y'  then acctamt_std*0.3
else acctamt_std*0.25 end)
where cnttype ='DRC'
and trandate between tran_date and tran_date_end;

END P1
