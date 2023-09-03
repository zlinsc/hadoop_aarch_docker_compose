#!/bin/zsh
## start standalone
#flink run -m localhost:8081 -c demo.MysqlCDCDemo target/flink_work-1.1.jar

# start app in yarn
export HADOOP_CLASSPATH=`hadoop classpath`
flink run-application -t yarn-application -Dyarn.application.name=cdctest -Dclient.timeout=600s -c demo.MysqlCDCDemo flink_work-1.1.jar

# recover from checkpoint
export HADOOP_CLASSPATH=`hadoop classpath`
flink run-application -t yarn-application -Dyarn.application.name=cdctest \
  -s hdfs://master-node:50070/user/root/checkpoints/7a58042487da30bbc2b9cbcf28d5a2cb/chk-1 \
  -Dclient.timeout=600s -c demo.MysqlCDCDemo flink_work-1.1.jar
