#!/bin/sh
while read line
do
	echo $line
	sed -i 's/mypasswd/mypasswd/g' $line
done<del.txt
