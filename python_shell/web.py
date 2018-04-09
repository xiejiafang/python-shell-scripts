#!/usr/bin/python
#coding:utf-8
import commands
import sys
import pyodbc
import multiprocessing
import time


#global var
months = 2


#承保业绩
def acct():
    #建立数据库连接
    conn=pyodbc.connect('DRIVER={DB2};SERVER=192.168.1.1;DATABASE=HNII;UID=db2inst;mypasswd&;charset=utf-8')
    cur = conn.cursor()
    print("delete from hn_web_acct...")
    curr_sql= "delete from administrator.hn_web_acct where month BETWEEN int(decimal(CURRENT date - %s months)/100) AND int(decimal(CURRENT date)/100)" % months
    cur.execute(curr_sql.decode('utf-8')) 
    print("insert into hn_web_acct...")
    curr_sql="""
	INSERT INTO administrator.hn_web_acct
	SELECT
	aracde,
	CASE WHEN agtype='RC' THEN 'SZ' ELSE 'YX' END AS series,
	int(trandate/100) AS MONTH,
	cnttype,
	sum(acctamt_std) AS bf,
	count(CASE WHEN batc_type='CB' THEN chdrnum end)-count(CASE WHEN batc_type='CD' THEN chdrnum end)-count(CASE WHEN batc_type='FH' THEN chdrnum end) AS js
	FROM administrator.hn_acctinfo
	WHERE agtype in(select agtype from administrator.hn_agtype)
	AND trandate BETWEEN decimal(CURRENT date - %s months) AND decimal(CURRENT date)
	GROUP BY cnttype,aracde,CASE WHEN agtype='RC' THEN 'SZ' ELSE 'YX' END,int(trandate/100)
    """ % months
    cur.execute(curr_sql.decode('utf-8')) 
    cur.commit()			
    cur.close()
    print("acct finished!")

#实收保费
def xuqiacct():
    #建立数据库连接
    conn=pyodbc.connect('DRIVER={DB2};SERVER=192.168.1.1;DATABASE=HNII;UID=db2inst;mypasswd&;charset=utf-8')
    cur = conn.cursor()
    print("delete from hn_web_xuqiacct...")
    curr_sql= "delete from administrator.hn_web_xuqiacct where month BETWEEN int(decimal(CURRENT date - %s months)/100) AND int(decimal(CURRENT date)/100)" % months
    cur.execute(curr_sql.decode('utf-8')) 
    print("insert into hn_web_xuqiacct...")
    curr_sql="""
	INSERT INTO administrator.hn_web_xuqiacct  
	SELECT
	agntara,
	int(trandate/100) AS MONTH,
	sum(acctamt) AS bf
	FROM administrator.hn_xuqiacct
	WHERE agtype in(select agtype from administrator.hn_agtype)
	AND trandate BETWEEN decimal(CURRENT date - %s months) AND decimal(CURRENT date)
	GROUP BY agntara,int(trandate/100)
    """ % months
    cur.execute(curr_sql.decode('utf-8')) 
    cur.commit()			
    cur.close()
    print("xuqiacct finished!")

def cnttype():
    #建立数据库连接
    conn=pyodbc.connect('DRIVER={DB2};SERVER=192.168.1.1;DATABASE=HNII;UID=db2inst;mypasswd&;charset=utf-8')
    cur = conn.cursor()
    print("delete from hn_web_cnttype...")
    curr_sql= "delete from administrator.hn_web_cnttype where month BETWEEN int(decimal(CURRENT date - %s months)/100) AND int(decimal(CURRENT date)/100)" % months
    cur.execute(curr_sql.decode('utf-8')) 
    print("insert into hn_web_cnttype...")
    curr_sql="""
	SELECT
	row_number()over(PARTITION BY int(trandate/100) ORDER BY sum(acctamt_std) desc) AS de_RANK,  
	cnttype,
	int(trandate/100) AS MON,
	sum(acctamt_std) AS bf,
	count(CASE WHEN batc_type='CB' THEN chdrnum end)-count(CASE WHEN batc_type='CD' THEN chdrnum end)-count(CASE WHEN batc_type='FH' THEN chdrnum end) AS js
	FROM administrator.hn_acctinfo
	WHERE agtype in(select agtype from administrator.hn_agtype)
	AND trandate BETWEEN decimal(CURRENT date - %s months) AND decimal(CURRENT date)
	GROUP BY cnttype,int(trandate/100)
    """ % months
    cur.execute(curr_sql.decode('utf-8')) 
    cur.commit()			
    cur.close()
    print("cnttype finished!")

def hdrl():
    #建立数据库连接
    conn=pyodbc.connect('DRIVER={DB2};SERVER=192.168.1.1;DATABASE=HNII;UID=db2inst;mypasswd&;charset=utf-8')
    cur = conn.cursor()
    print("delete from hn_web_hdrl...")
    curr_sql= "delete from administrator.hn_web_hdrl where month BETWEEN int(decimal(CURRENT date - %s months)/100) AND int(decimal(CURRENT date)/100)" % months
    cur.execute(curr_sql.decode('utf-8')) 
    print("insert into hn_web_hdrl...")
    curr_sql="""
	SELECT
	aracde,
	agntnum,
	agtype,
	int(trandate/100) AS MON,
	sum(acctamt_std) AS bf,
	count(CASE WHEN batc_type='CB' THEN chdrnum end)-count(CASE WHEN batc_type='CD' THEN chdrnum end)-count(CASE WHEN batc_type='FH' THEN chdrnum end) AS js
	FROM administrator.hn_acctinfo
	WHERE agtype in(select agtype from administrator.hn_agtype)
	AND trandate BETWEEN decimal(CURRENT date - %s months) AND decimal(CURRENT date)
	GROUP BY aracde,int(trandate/100),agntnum,agtype
    """ % months
    cur.execute(curr_sql.decode('utf-8')) 
    cur.commit()			
    cur.close()
    print("hdrl finished!")

def rl():
    #建立数据库连接
    conn=pyodbc.connect('DRIVER={DB2};SERVER=192.168.1.1;DATABASE=HNII;UID=db2inst;mypasswd&;charset=utf-8')
    cur = conn.cursor()
    print("delete from hn_web_rl...")
    curr_sql= "delete from administrator.hn_web_rl "
    cur.execute(curr_sql.decode('utf-8')) 
    print("insert into hn_web_rl...")
    curr_sql="""
	INSERT INTO administrator.hn_web_rl  
	SELECT
	aracde,
	'当前人力',
	count(agntnum) AS rl
	FROM administrator.hn_agntinfo
	WHERE dtetrm=99999999
	AND agtype in(select agtype from administrator.hn_agtype)
	GROUP BY aracde
	UNION all      
	SELECT
	aracde,
	'月初人力',
	count(agntnum) AS rl
	FROM administrator.hn_agntinfo
	WHERE dtetrm>=decimal(current date - day(current date -1 days) days) AND dteapp<decimal(current date - day(current date -1 days) days)
	AND agtype in(select agtype from administrator.hn_agtype)
	GROUP BY aracde      
	UNION all
	SELECT
	aracde,
	'月在职人力',
	count(agntnum) AS rl
	FROM administrator.hn_agntinfo
	WHERE dtetrm>=decimal(current date - day(current date -1 days) days) AND dteapp<=decimal(current date)
	AND agtype in(select agtype from administrator.hn_agtype)
	GROUP BY aracde       
    """
    cur.execute(curr_sql.decode('utf-8')) 
    cur.commit()			
    cur.close()
    print("rl finished!")


def rl_in():
    #建立数据库连接
    conn=pyodbc.connect('DRIVER={DB2};SERVER=192.168.1.1;DATABASE=HNII;UID=db2inst;mypasswd&;charset=utf-8')
    cur = conn.cursor()
    print("delete from hn_web_rl_in...")
    curr_sql= "delete from administrator.hn_web_rl_in where month BETWEEN int(decimal(CURRENT date - %s months)/100) AND int(decimal(CURRENT date)/100)" % months
    cur.execute(curr_sql.decode('utf-8')) 
    print("insert into hn_web_rl_in...")
    curr_sql="""
	INSERT INTO administrator.hn_web_rl_in        
	SELECT
	aracde,
	int(dteapp/100),
	count(agntnum) AS rl
	FROM administrator.hn_agntinfo
	WHERE agtype in(select agtype from administrator.hn_agtype)
	AND dteapp BETWEEN decimal(CURRENT date - %s months) AND decimal(CURRENT date)
	GROUP BY aracde,int(dteapp/100) 
    """ % months
    cur.execute(curr_sql.decode('utf-8')) 
    cur.commit()			
    cur.close()
    print("rl_in finished!")

def rl_out():
    #建立数据库连接
    conn=pyodbc.connect('DRIVER={DB2};SERVER=192.168.1.1;DATABASE=HNII;UID=db2inst;mypasswd&;charset=utf-8')
    cur = conn.cursor()
    print("delete from hn_web_rl_out...")
    curr_sql= "delete from administrator.hn_web_rl_out where month BETWEEN int(decimal(CURRENT date - %s months)/100) AND int(decimal(CURRENT date)/100)" % months
    cur.execute(curr_sql.decode('utf-8')) 
    print("insert into hn_web_rl_out...")
    curr_sql="""
	INSERT INTO administrator.hn_web_rl_out        
	SELECT
	aracde,
	int(dtetrm/100),
	count(agntnum) AS rl
	FROM administrator.hn_agntinfo
	WHERE agtype in(select agtype from administrator.hn_agtype)
	AND dteapp BETWEEN decimal(CURRENT date - %s months) AND decimal(CURRENT date)
	GROUP BY aracde,int(dtetrm/100) 
    """ % months
    cur.execute(curr_sql.decode('utf-8')) 
    cur.commit()			
    cur.close()
    print("rl_out finished!")

if __name__ == "__main__":
    if int(time.strftime("%H",time.localtime()))==8:
	p1 = multiprocessing.Process(target = xuqiacct)
	p1.start()
    p2 = multiprocessing.Process(target = acct)
    p2.start()
    p3 = multiprocessing.Process(target = cnttype)
    p3.start()
    p4 = multiprocessing.Process(target = hdrl)
    p4.start()
    p5 = multiprocessing.Process(target = rl)
    p5.start()
    p6 = multiprocessing.Process(target = rl_in)
    p6.start()
    p7 = multiprocessing.Process(target = rl_out)
    p7.start()




