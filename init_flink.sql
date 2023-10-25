-- sql-client.sh gateway -e localhost:8083
-- SET 'execution.checkpointing.interval' = '30s';

-- CREATE TABLE postgres_cdc_users (
--   user_id INT NOT NULL,
--   name STRING NOT NULL,
--   age INT,
--   locked BOOLEAN NOT NULL,
--   created_at TIMESTAMP NOT NULL,
--   PRIMARY KEY (`user_id`) NOT ENFORCED
-- ) WITH (
--   'connector' = 'postgres-cdc',    
--   'hostname' = 'db-node',          
--   'port' = '5432',                 
--   'username' = 'postgres',       
--   'password' = '123456',           
--   'database-name' = 'test_db',     
--   'schema-name' = 'public',
--   'table-name' = 'users',           
--   'slot.name' = 'slot4users',
--   'decoding.plugin.name' = 'pgoutput'
-- );
-- select * from postgres_cdc_users;

-- create table mysql_cdc_orders (
--   order_id string,
--   order_channel string,
--   order_time string,
--   pay_amount double,
--   real_pay double,
--   pay_time string,
--   user_id string,
--   user_name string,
--   area_id STRING,
--   PRIMARY KEY (order_id) NOT ENFORCED
-- ) with (
--   'connector' = 'mysql-cdc',
--   'hostname' = 'db-node',
--   'username' = 'root',
--   'password' = '123456',
--   'database-name' = 'test_db',
--   'table-name' = 'cdc_order'
-- );
-- select * from mysql_cdc_orders;

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


-- yarn-session.sh --detached -Dyarn.application.name=flinksql -Dparallelism.default=1 -Dtaskmanager.numberOfTaskSlots=1 -Dtaskmanager.memory.process.size=2gb -Djobmanager.memory.process.size=2gb -Dclient.timeout=600s
-- sql-client.sh embedded -i /home/init_flink.sql -s yarn-session
CREATE TABLE cdc_order_hudi (
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
)
WITH (
  'connector' = 'hudi',
  'path' = 'hdfs://master-node:50070/tmp/cdc_order_hudi',
  'table.type' = 'MERGE_ON_READ',
  'hoodie.datasource.write.recordkey.field' = 'id',
  'hoodie.datasource.write.hive_style_partitioning' = 'true'
);
-- select * from cdc_order_hudi;
-- insert into cdc_order_hudi values ('ab1003', 'ad', '2023-07-02', 16.2, 24, '2023-07-01 12:34:01', 'jk1089463', 'jackma', 'gz');
