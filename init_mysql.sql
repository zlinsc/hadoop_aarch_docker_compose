-- mysql -hmysql-node -uroot -proot -P3306 < mysql_init.sql

CREATE USER 'root'@'%' IDENTIFIED BY '123456';
GRANT ALL PRIVILEGES ON *.* TO 'root'@'%';

create database test_db;

CREATE TABLE test_db.cdc_order (
	`order_id` VARCHAR(32) NOT NULL,
	`order_channel` VARCHAR(32) NULL,
	`order_time` VARCHAR(32) NULL,
	`pay_amount` DOUBLE  NULL,
	`real_pay` DOUBLE  NULL,
	`pay_time` VARCHAR(32) NULL,
	`user_id` VARCHAR(32) NULL,
	`user_name` VARCHAR(32) NULL,
	`area_id` VARCHAR(32) NULL,
	PRIMARY KEY (`order_id`)
)	ENGINE = InnoDB
	DEFAULT CHARSET=utf8;
insert into test_db.cdc_order values ('ab1000', 'ad', '2023-07-01', 56.4, 32.4, '2023-07-01 12:33:09', 'jk1089463', 'jack', 'gz');
insert into test_db.cdc_order values ('ab1001', 'ad', '2023-07-02', 16.2, 24, '2023-07-01 12:34:01', 'jk1089463', 'jackma', 'gz');
update test_db.cdc_order set pay_amount=30, real_pay=10 where order_id='ab1001';
insert into test_db.cdc_order values ('ab1002', 'ad', '2023-07-02', 16.2, 24, '2023-07-03 12:30:12', 'jk1089463', 'kk', 'sz');
delete from test_db.cdc_order where order_id='ab1002';
update test_db.cdc_order set pay_amount=56.6, real_pay=44.4 where order_id='ab1000';

-- alter table test_db.cdc_order ADD email VARCHAR(255);
-- insert into test_db.cdc_order values ('ab1010', 'ad', '2023-07-02', 16.2, 24, '2023-07-03 12:30:12', 'jk1089463', 'kk', 'sz', 'zz@qq.com');
-- alter table test_db.cdc_order modify COLUMN user_id int;
-- alter table test_db.cdc_order DROP COLUMN user_name;
-- insert into test_db.cdc_order values ('ab1011', 'ad', '2023-07-02', 16.2, 24, '2023-07-03 12:30:12', 'jk1089463', 'sz', 'xxx@qq.com');