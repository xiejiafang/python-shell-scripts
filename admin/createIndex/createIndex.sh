#!/bin/sh
db2 connect to hnii	
while read line
do
	col=$line
	indexname=`echo $col | awk '{print $1}'`
	tablename=`echo $col | awk '{print $2}'`
	colname=`echo $col | awk '{print $3}'`
	echo $indexname,$tablename,colname
	db2  "create index index.$indexname on administrator.$tablename($colname)"
done <$1
