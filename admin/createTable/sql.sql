#创建记录表
create table administrator.info_processtables
(
tabname   varchar(30),
operation char(1),
rowcount decimal(10,0),
rownew decimal(8,0),
processtime decimal(8,0),
operdate        timestamp
)
