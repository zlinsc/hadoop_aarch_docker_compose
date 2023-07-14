-- SET 'execution.checkpointing.interval' = '30s';
-- sql-client.sh gateway -e localhost:8083


CREATE TABLE postgres_cdc_users (
  user_id INT NOT NULL,
  name STRING NOT NULL,
  age INT,
  locked BOOLEAN NOT NULL,
  created_at TIMESTAMP NOT NULL,
  PRIMARY KEY (`user_id`) NOT ENFORCED
) WITH (
  'connector' = 'postgres-cdc',    
  'hostname' = 'db-node',          
  'port' = '5432',                 
  'username' = 'postgres',       
  'password' = '123456',           
  'database-name' = 'test_db',     
  'schema-name' = 'public',
  'table-name' = 'users',           
  'slot.name' = 'slot4users',
  'decoding.plugin.name' = 'pgoutput'
);
select * from postgres_cdc_users;


create table mysql_cdc_orders (
  order_id string,
  order_channel string,
  order_time string,
  pay_amount double,
  real_pay double,
  pay_time string,
  user_id string,
  user_name string,
  area_id STRING,
  PRIMARY KEY (order_id) NOT ENFORCED
) with (
  'connector' = 'mysql-cdc',
  'hostname' = 'db-node',
  'username' = 'root',
  'password' = '123456',
  'database-name' = 'test_db',
  'table-name' = 'cdc_order'
);
select * from mysql_cdc_orders;


-- create table printSink(
--   order_id string,
--   order_channel string,
--   order_time string,
--   pay_amount double,
--   real_pay double,
--   pay_time string,
--   user_id string,
--   user_name string,
--   area_id STRING
-- ) with (
--   'connector' = 'print'
-- );
-- insert into printSink select * from mysqlCdcSource;

