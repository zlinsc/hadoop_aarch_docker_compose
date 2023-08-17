#!/bin/zsh
export HADOOP_CLASSPATH=`hadoop classpath`
flink run-application -t yarn-application -Dyarn.provided.lib.dirs=hdfs://ctyunns/user/ads/flink/lib -Dclient.timeout=600s \
 -Dyarn.application.name=cust.PROD_SPEC_INST_ATTR.0 -Dyarn.application.queue=ads \
 -Djobmanager.rpc.num-task-slots=4 -Djobmanager.cpus=4 -Djobmanager.memory.process.size=4gb \
 -Dparallelism.default=4 -Dtaskmanager.memory.process.size=8gb -Dtaskmanager.numberOfTaskSlots=1 \
 -c online.CustMysqlCDC flink_work-1.1.jar cust.PROD_SPEC_INST_ATTR.0