#!/bin/zsh
## start standalone
#flink run -m localhost:8081 -c demo.MysqlCDCDemo target/flink_work-1.1.jar

# export HADOOP_CLASSPATH=`hadoop classpath`

# start app in yarn
# flink run-application -t yarn-application -Dyarn.application.name=cdctest \
#   -Dparallelism.default=1 -Dtaskmanager.numberOfTaskSlots=1 \
#   -Dtaskmanager.memory.process.size=1gb -Djobmanager.memory.process.size=1gb \
#   -Dtaskmanager.memory.managed.fraction=0.1 \
#   -Dclient.timeout=600s -c demo.MysqlCDCDemo flink_work-1.1.jar
# flink run-application -t yarn-application -Dyarn.application.name=cdctest \
#   -Dparallelism.default=1 -Dtaskmanager.numberOfTaskSlots=1 \
#   -Dtaskmanager.memory.process.size=1gb -Djobmanager.memory.process.size=1gb \
#   -Dtaskmanager.memory.managed.fraction=0.1 \
#   -Dclient.timeout=600s -c demo.HudiDemo flink_work-1.1.jar
flink run -t yarn-session --detached -Dyarn.application.id=application_1697441862637_0001 \
  -Dparallelism.default=1 -c demo.MysqlCDCDemo flink_work-1.1.jar
flink run -t yarn-session --detached -Dyarn.application.id=application_1697441862637_0001 \
  -Dparallelism.default=1 -c demo.HudiDemo flink_work-1.1.jar

# recover from checkpoint
flink run-application -t yarn-application -Dyarn.application.name=cdctest \
  -s hdfs://master-node:50070/user/root/checkpoints/7a58042487da30bbc2b9cbcf28d5a2cb/chk-1 \
  -Dclient.timeout=600s -c demo.MysqlCDCDemo flink_work-1.1.jar
