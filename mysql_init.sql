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
