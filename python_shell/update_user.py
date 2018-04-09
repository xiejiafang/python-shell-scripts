#!/usr/bin/python
#coding:utf-8
import commands
import sys
import pyodbc

#设置数据库连接
conn=pyodbc.connect('DRIVER={DB2};SERVER=192.168.1.1;DATABASE=HNII;UID=db2inst;mypasswd&;charset=utf-8')
cur = conn.cursor()

##更新用户
curr_sql= 'delete from web.report_user '
cur.execute(curr_sql.decode('utf-8')) 
curr_sql="insert into web.report_user select * from db_105.report_user"
cur.execute(curr_sql.decode('utf-8')) 
cur.commit()			

curr_sql= 'update db_33.report_user a set password=(select password from web.report_user b where a.username=b.username)'
cur.execute(curr_sql.decode('utf-8')) 
curr_sql="""
insert into db_33.report_user (first_name,username,password)
select  first_name,username,password  from web.report_user a
  where not exists(select * from db_33.report_user b where a.username=b.username)
"""
cur.execute(curr_sql.decode('utf-8')) 
curr_sql="""
insert into db_33.report_membership(user_id,group_id) select id,1 from db_33.report_user where username='it'
"""
cur.execute(curr_sql.decode('utf-8')) 


cur.commit()			
cur.close()

