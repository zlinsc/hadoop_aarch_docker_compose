#!/bin/zsh
# $1 is the main class name
flink run -m localhost:8081 -c $1 target/flink_work-1.0.jar
#flink run-application -t yarn-application -Dclient.timeout=600s -c MysqlCDCDemo flink_work-1.0.jar
