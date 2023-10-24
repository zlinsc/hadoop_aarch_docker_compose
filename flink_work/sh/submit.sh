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


##############################################################
function producer_startup() {
    export HADOOP_CLASSPATH=`hadoop classpath`
    ./flink-1.17.0/bin/flink run-application -t yarn-application -Dyarn.provided.lib.dirs=hdfs://ctyunns/user/ads/flink/lib \
        -Dsecurity.kerberos.login.use-ticket-cache=true -Dsecurity.kerberos.login.contexts=Client,KafkaClient \
        -Dsecurity.kerberos.login.principal=ads/hdp-tmp007.nm.ctdcp.com@BIGDATA.CHINATELECOM.CN \
        -Dsecurity.kerberos.login.keytab=/etc/security/keytabs/ads.keytab \
        -Djobmanager.archive.fs.dir=hdfs://ctyunns/flink-history/realjob \
        -Dyarn.application.queue=$5 -Dclient.timeout=600s \
        -Dparallelism.default=$3 -Dtaskmanager.numberOfTaskSlots=1 \
        -Dtaskmanager.memory.process.size=$4 -Djobmanager.memory.process.size=1gb \
        -Dtaskmanager.memory.managed.fraction=0.1 \
        -Dyarn.application.name=p#$1 \
        -c MysqlCDCProducer flink_work-1.1.jar $1 $2
#        -c MysqlCDCProducer flink_work-1.1.jar $1 $2 $4
}
#producer_startup "cust.0.PROD_SPEC_INST_ATTR" 4 pos/mysql-bin.000001:5997
#producer_startup "cust.0.PROD_SPEC_INST_ATTR" 4 gtid/6dc8b5af-1616-11ec-8f60-a4ae12fe8402:1-20083670,6de8242f-1616-11ec-94a2-a4ae12fe9796:1-700110909

function loop_producer_startup() {
  local db="$1"
  local table="$2"
  local serverid="$3"
  local parall="$4"
  local gb="$5"
  local queue="$6"
  local n=8

  for ((i = 0; i < n; i++)); do
    producer_startup "${db}.${i}.${table}" ${serverid} ${parall} ${gb} ${queue}
  done
}


#######################################################
#function get_app_id() {
#  if [ $# -ne 1 ]; then
#    echo "Usage: $0 <app_name>"
#    exit 1
#  fi
#  MAX_NUMBER=1
#  for ((i = 0; i < MAX_NUMBER; i++)); do
#    APP_NAME="$1"
#    APP_ID=$(yarn application -list | grep "$APP_NAME" | awk '{print $1}')
#    echo "$APP_ID"
#  done
#}
#
#function producer_unified_startup() {
#    export HADOOP_CLASSPATH=`hadoop classpath`
#    ./flink-1.17.0/bin/yarn-session.sh --detached -Dyarn.provided.lib.dirs=hdfs://ctyunns/user/ads/flink/lib \
#        -Dsecurity.kerberos.login.use-ticket-cache=true -Dsecurity.kerberos.login.contexts=Client,KafkaClient \
#        -Dsecurity.kerberos.login.principal=ads/hdp-tmp007.nm.ctdcp.com@BIGDATA.CHINATELECOM.CN \
#        -Dsecurity.kerberos.login.keytab=/etc/security/keytabs/ads.keytab \
#        -Djobmanager.archive.fs.dir=hdfs://ctyunns/flink-history/realjob \
#        -Dhistoryserver.archive.fs.dir=hdfs://ctyunns/flink-history/realjob \
#        -Dyarn.application.queue=ads -Dclient.timeout=600s \
#        -Djobmanager.memory.process.size=4gb -Dtaskmanager.memory.process.size=4gb -Dtaskmanager.numberOfTaskSlots=4 \
#        -Dyarn.application.name=p#$1
#    # echo "stop" | ./flink-1.17.0/bin/yarn-session.sh -id application_1682213308051_1374
#}
## producer_unified_startup cust.PROD_SPEC_INST_ATTR
#
#function producer_startup() {
#    if [ $# -eq 3 ]; then
#        param="$2 $3"
#    elif [ $# -eq 4 ]; then
#        param="$2 $3 $4"
#    else
#        echo "table serverid parallel [recover]"
#        return 1
#    fi
#
#    export HADOOP_CLASSPATH=`hadoop classpath`
#    appid=$(get_app_id "flinksql")
#    if [ -n "$appid" ]; then
#        ./flink-1.17.0/bin/flink run -t yarn-session --detached -Dyarn.application.id=$appid \
#                -Dparallelism.default=$1 -c MysqlCDCProducer flink_work-1.1.jar $param
#    fi
#    # ./flink-1.17.0/bin/flink cancel -t yarn-session -Dyarn.application.id=application_1682213308051_1379 {jobid}
#}
##producer_startup "cust.0.PROD_SPEC_INST_ATTR" 4 pos/mysql-bin.000001:5997
##producer_startup "cust.0.PROD_SPEC_INST_ATTR" 4 gtid/6dc8b5af-1616-11ec-8f60-a4ae12fe8402:1-20083670,6de8242f-1616-11ec-94a2-a4ae12fe9796:1-700110909
#
#function loop_producer_startup() {
#  local db="$1"
#  local table="$2"
#  local serverid="$3"
#  local paral="$4"
#  local n="$5"
#
#  for ((i = 0; i < n; i++)); do
#    producer_startup ${paral} "${db}.${i}.${table}" ${serverid}
#  done
#}
##loop_producer_startup "acctdb" "order_info_log" "5401" 1 8
##loop_producer_startup "acctdb" "acct_item_total_month_[0-9]{6}" "5411" 1 8
