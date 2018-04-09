#!/bin/sh
while read line
do
 kill -9 $line
done<ps.txt
