#!/bin/sh
db2 connect to hnii
while read line
do
  echo "creating nickname $line:"
  db2 "drop nickname administrator.$line"
  db2 "create nickname administrator.$line for tk_90_1.zhengzh.$line"
  db2 "grant select on administrator.$line to user report"
done < $1
