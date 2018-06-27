import config
from cassandra.cluster import Cluster

# Connect to cassandra cluster
cluster = Cluster([config.cass_seedip])
session = cluster.connect()

# Create and set cassandra keyspace to work
session.execute("CREATE KEYSPACE"+ config.cass_keyspace +"WITH replication = {'class':" + config.cass_class+", 'replication_factor':"+config.cass_rf+"};")
session.set_keyspace(config.cass_keyspace)

# Create tables to insert data
session.execute("CREATE TABLE user_song_log (timestamp int, user_id int, song_id int, primary key(timestamp, user_id));")



# Close the connection
#session.shutdown()
#cluster.shutdown()

