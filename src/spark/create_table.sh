#!/bin/bash
# first create keyspace with the following command in cqlsh shell
#cqlsh> CREATE KEYSPACE test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
# then create table
cqlsh -e'use test; CREATE Table protable ( achannel text, busername text, ccount bigint, PRIMARY KEY (achannel, busername), );'
cqlsh -e'use test; CREATE Table rawtable ( ausername text, bmessage text, cchannel text, dtime text, PRIMARY KEY (ausername, cchannel), );'
