create database test_db;

CREATE TABLE `test_db`.`cdc_order` (
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
	DEFAULT CHARACTER SET = utf8mb4
	COLLATE = utf8mb4_general_ci;
