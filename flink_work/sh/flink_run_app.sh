#!/bin/bash
export HADOOP_CLASSPATH=$(hadoop classpath)

flink_bin_dir=./flink-1.17.0-x/bin/
kafka_kerberos_conf="-Dsecurity.kerberos.login.use-ticket-cache=true -Dsecurity.kerberos.login.contexts=Client,KafkaClient
                     -Dsecurity.kerberos.login.principal=ads/hdp-tmp007.nm.ctdcp.com@BIGDATA.CHINATELECOM.CN
                     -Dsecurity.kerberos.login.keytab=/etc/security/keytabs/ads.keytab"
flink_default_conf="-Dyarn.provided.lib.dirs=hdfs://ctyunns/user/ads/flink/lib2
                    -Dclient.timeout=600s -Dtaskmanager.slot.timeout=300s -Dakka.ask.timeout=300s
                    -Djobmanager.archive.fs.dir=hdfs://ctyunns/flink-history/realjob
                    -Dhistoryserver.archive.fs.dir=hdfs://ctyunns/flink-history/realjob"

function get_appid_using_name() {
  local APP_NAME=$1
  for ((i = 0; i < 1; i++)); do
    local APP_ID=$(yarn application -list | grep "$APP_NAME" | awk '{print $1}')
    echo "$APP_ID"
  done
}

function cdc_startup_app() {
  local APP_NAME=$1
  local QUEUE=$2
  local TM_MEM=$3
  set -x
  $flink_bin_dir/yarn-session.sh --detached $kafka_kerberos_conf $flink_default_conf \
    -Dyarn.application.name=p2#$APP_NAME -Dyarn.application.queue=$QUEUE \
    -Djobmanager.memory.process.size=1gb -Dtaskmanager.numberOfTaskSlots=1 \
    -Dtaskmanager.memory.process.size=$TM_MEM -Dtaskmanager.memory.managed.fraction=0.1
  set +x
}
#cdc_startup_app "order.INNER_ORD_PROD_SPEC_INST_HIS" "ads" "4gb"

function cdc_startup_job() {
  local APP_NAME=$1
  local PARAMS="$2"
  local APP_ID=$(get_appid_using_name "$APP_NAME")
  if [ -n "$APP_ID" ]; then
    set -x
    $flink_bin_dir/flink run -t yarn-session --detached -Dyarn.application.id=$APP_ID \
      -Dclassloader.check-leaked-classloader=false -Dparallelism.default=1 -c MysqlCDC2Hudi flink_work-1.2.jar $PARAMS
    set +x
  else
    echo "No application found with name $APP_NAME"
  fi
}
cdc_startup_job "p2#order.INNER_ORD_PROD_SPEC_INST_HIS" "order.0.INNER_ORD_PROD_SPEC_INST_HIS 5645-5648"