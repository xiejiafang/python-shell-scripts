
#!/bin/sh
#定义命令路径
ph='/home/db2inst/sqllib/bin'


db2 "alter table administrator.hn_ycb_rtrn activate not logged initially with empty table"
db2 "insert into administrator.hn_ycb_rtrn select * from administrator.v_ycb_rtrn"
db2 "alter table administrator.hn_ycb_rtrn_day activate not logged initially with empty table"
db2 "insert into administrator.hn_ycb_rtrn select * from administrator.v_ycb_rtrn_day"
db2 "alter table administrator.hn_ycb_hpad activate not logged initially with empty table"
db2 "insert into administrator.hn_ycb_hpad select * from administrator.v_ycb_hpad"
db2 "alter table administrator.hn_ycb_hpad_day activate not logged initially with empty table"
db2 "insert into administrator.hn_ycb_hpad_day select * from administrator.v_ycb_hpad_day"
 db2 connect reset
