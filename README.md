MATERIALIZED VIEW BENCHMARK
===========================

### Summary

This tools will run a synthetic workload against Apache Cassandra
with the intention of stressing the system.  It can maintain multiple
views of data either manually (in app) or via Materialized Views (in server)

### Workload Story

Imagine we run a music service that allows users to create playlists.
The core table for our service is:
  ````
CREATE TABLE user_playlists
(
    user_name           text,
    playlist_name       text,
    song_id             text,
    added_time          bigint,
    artist_name         text,
    genre               text,
    last_played         bigint,
    PRIMARY KEY (user_name, playlist_name, song_id) 
);

  ````

Now with this information we can create add, update, delete playlists in our system.
But we want to answer other questions using this data like:

   Which users like the same song?
   Which users like the same artist?
   Which users like the same genre?
   What was the recently played tracks for a given user?

So our system maintains other views on this data (defined in the schema)


### How to use?

````
   # Load the schema into your C* cluster
   cqlsh < bench_schema.cql
   
   # compile 
   mvn compile
   
   # run with server based materialized view mode
   mvn exec:java -Dexec.args=""

   # To with in app based manual view mode
   mvn exec:java -Dexec.args="--manual" 

   # To see all options
   mvn exec:java -Dexec.args="--help"
   
````

After running, there will be a detailed csv files under ./reports/view and ./reports/manual


by [@tjake](http://twitter.com/tjake)
