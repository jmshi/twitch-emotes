#!/bin/bash
/usr/local/cassandra/bin/nodetool status
/usr/local/cassandra/bin/nodetool ring |more


# check for existing keyspace
SELECT table_name 
FROM system_schema.tables WHERE keyspace_name='myKeyspaceName';
