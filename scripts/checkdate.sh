#!/bin/sh

while [ 1 == 1 ]
do
  echo `date`
  hpad_day=`db2 "select count(*) from administrator.v_ycb_hpad_day"`
  echo "hpad_day:"
  echo $hpad_day
  rtrn_day=`db2 "select count(*) from administrator.v_ycb_rtrn_day"`
  echo "rtrn_day:"
  echo $rtrn_day
  sleep 60
done
db2 connect reset
