#!/usr/bin/python
#coding:utf-8
import os
import commands
os.system("db2 connect to hnii")
str = commands.getoutput("db2 list application |grep ^REPORT |awk '{print $3}'")
str = str.split("\n")
for num in str:
	sql = "db2 'force application ("+num+")'"
	print(sql)
	os.system(sql)
os.system("db2 connect reset")

