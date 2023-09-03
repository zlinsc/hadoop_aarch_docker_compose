#!/bin/sh
export HADOOP_CLASSPATH=`hadoop classpath`
flink run-application -t yarn-application -Dyarn.application.name=cdctest -Djob.id=cdctest -Dclient.timeout=600s -c demo.MysqlCDCDemo flink_work-1.1.jar