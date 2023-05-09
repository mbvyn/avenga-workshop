CREATE KEYSPACE IF NOT EXISTS bikes WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

USE bikes;

CREATE TABLE IF NOT EXISTS max_duration_bike (
   max_date date,
   bike_id int,
   max_duration int,
   PRIMARY KEY ((max_date), bike_id)
);
