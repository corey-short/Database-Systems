#!/bin/bash
while read line; do
	python csv_helper.py $1
done < $1
exit 0
