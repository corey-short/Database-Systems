#!/bin/bash

URL="http://inst.eecs.berkeley.edu/~cs186/sp15/hw3_fec_data.zip"
#wget -nc $URL
#unzip -o hw3_fec_data.zip

#dropdb --if-exists fec
#createdb fec
#psql -d fec < schema.sql

BASE="C:/Users/Short/Desktop/repo/CS186/uc/hw3/part1/data"
#PATH="C:\Users\Short\Desktop\repo\CS186\uc\hw3\part1\data"
TABLES=(candidates committee_contributions committees individual_contributions intercommittee_transactions linkages)

for table in ${TABLES[@]}
do
    psql -e -d fec -c "\copy ${table} FROM '$BASE/${table}.txt' DELIMITER '|' CSV;"
done

psql -e -d fec -c "ANALYZE"
