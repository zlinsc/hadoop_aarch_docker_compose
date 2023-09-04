#!/bin/bash
#./flink-1.17.0/bin/flink run-application -t yarn-application -Dyarn.provided.lib.dirs="hdfs://ctyunns/user/ads/flink/lib" -class EmulatedDemo flink_work-1.0.jar
#./flink-1.17.0/bin/flink run-application -t yarn-application -Dyarn.provided.lib.dirs="hdfs://ctyunns/user/ads/flink/lib" -class PostgresCDCDemo flink_work-1.0.jar

function producer_startup() {
export HADOOP_CLASSPATH=`hadoop classpath`
./flink-1.17.0/bin/flink run-application -t yarn-application -Dyarn.provided.lib.dirs=hdfs://ctyunns/user/ads/flink/lib \
 -Dsecurity.kerberos.login.use-ticket-cache=true -Dsecurity.kerberos.login.contexts=Client,KafkaClient \
 -Dsecurity.kerberos.login.principal=caiyunjian/hdp-tmp007.nm.ctdcp.com@BIGDATA.CHINATELECOM.CN \
 -Dsecurity.kerberos.login.keytab=/etc/security/keytabs/caiyunjian.keytab \
 -Dyarn.application.queue=ads -Dclient.timeout=600s \
 -Djobmanager.rpc.num-task-slots=4 -Djobmanager.cpus=4 -Djobmanager.memory.process.size=4gb \
 -Dparallelism.default=4 -Dtaskmanager.memory.process.size=4gb -Dtaskmanager.numberOfTaskSlots=1 \
 -Dyarn.application.name=p#$1 \
 -c MysqlCDCProducer flink_work-1.1.jar $1
}
producer_startup "cust.PROD_SPEC_INST_ATTR.0"

function consumer_startup() {
export HADOOP_CLASSPATH=`hadoop classpath`
./flink-1.17.0/bin/flink run-application -t yarn-application -Dyarn.provided.lib.dirs=hdfs://ctyunns/user/ads/flink/lib \
 -Dsecurity.kerberos.login.use-ticket-cache=true -Dsecurity.kerberos.login.contexts=Client,KafkaClient \
 -Dsecurity.kerberos.login.principal=caiyunjian/hdp-tmp007.nm.ctdcp.com@BIGDATA.CHINATELECOM.CN \
 -Dsecurity.kerberos.login.keytab=/etc/security/keytabs/caiyunjian.keytab \
 -Dyarn.application.queue=ads -Dclient.timeout=600s \
 -Djobmanager.rpc.num-task-slots=4 -Djobmanager.cpus=4 -Djobmanager.memory.process.size=4gb \
 -Dparallelism.default=4 -Dtaskmanager.memory.process.size=4gb -Dtaskmanager.numberOfTaskSlots=1 \
 -Dyarn.application.name=c#$1 \
 -c CDCConsumer flink_work-1.1.jar $1 $2
}
consumer_startup "cust.PROD_SPEC_INST_ATTR"
consumer_startup "cust.PROD_SPEC_INST_ATTR" "2023-08-19_22:00:00"