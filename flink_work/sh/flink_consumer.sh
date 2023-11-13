#!/bin/bash
#./flink-1.17.0/bin/flink run-application -t yarn-application -Dyarn.provided.lib.dirs="hdfs://ctyunns/user/ads/flink/lib" -class EmulatedDemo flink_work-1.0.jar
#./flink-1.17.0/bin/flink run-application -t yarn-application -Dyarn.provided.lib.dirs="hdfs://ctyunns/user/ads/flink/lib" -class PostgresCDCDemo flink_work-1.0.jar

function consumer_startup() {
export HADOOP_CLASSPATH=`hadoop classpath`
./flink-1.17.0/bin/flink run-application -t yarn-application -Dyarn.provided.lib.dirs=hdfs://ctyunns/user/ads/flink/lib \
 -Dsecurity.kerberos.login.use-ticket-cache=true -Dsecurity.kerberos.login.contexts=Client,KafkaClient \
 -Dsecurity.kerberos.login.principal=ads/hdp-tmp007.nm.ctdcp.com@BIGDATA.CHINATELECOM.CN \
 -Dsecurity.kerberos.login.keytab=/etc/security/keytabs/ads.keytab \
 -Djobmanager.archive.fs.dir=hdfs://ctyunns/flink-history/realjob \
 -Dyarn.application.queue=ads -Dclient.timeout=600s \
 -Djobmanager.cpus=10 -Djobmanager.memory.process.size=4gb \
 -Dparallelism.default=10 -Dtaskmanager.memory.process.size=4gb -Dtaskmanager.numberOfTaskSlots=1 \
 -Dmetrics.reporters=prom \
 -Dmetrics.reporter.prom.factory.class=org.apache.flink.metrics.prometheus.PrometheusReporterFactory \
 -Dyarn.application.name=c#$1 \
 -c TestCDCConsumer flink_work-1.1.jar $1 $2
}
#consumer_startup "cust.PROD_SPEC_INST_ATTR" "2023-09-19_11:30:00"
