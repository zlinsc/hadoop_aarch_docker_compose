#!/bin/bash
jar_file_name='flink_work-1.4.jar'

function get_app_id() {
  if [ $# -ne 1 ]; then
    echo "Usage: $0 <app_name>"
    exit 1
  fi
  MAX_NUMBER=1
  for ((i = 0; i < MAX_NUMBER; i++)); do
    APP_NAME="$1"
    APP_ID=$(yarn application -list | grep "$APP_NAME" | awk '{print $1}')
    echo "$APP_ID"
  done
}

function kill_app() {
  app_name=$1
  appid=$(get_app_id "$app_name")
  if [ -n "$appid" ]; then
   yarn application -kill $appid
  fi
}

function flink_read_state() {
  local chk_path=$1
  TASK_NAME="flink_read_state"

  command="flink run-application -t yarn-application -Dclient.timeout=600s -Dparallelism.default=1 -Dtaskmanager.numberOfTaskSlots=1 \
   -Dtaskmanager.memory.process.size=1gb -Djobmanager.memory.process.size=1gb -Dtaskmanager.memory.managed.fraction=0.1 \
   -Dyarn.application.name=$TASK_NAME -c lakepump.cdc.CDCStateReader $jar_file_name $chk_path"
  echo -e "\033[30;44m[read gtid]\033[0m \033[34;4m${command}\033[0m"
  APP_ID=$($command | grep "Submitted application" | awk -F" " '{print $9}')
  echo "appid: $APP_ID"

  while true; do
    APP_INFO=$(yarn application -list -appStates ALL | grep "$APP_ID" | awk -F" " '{print $7}' | grep 'RUNNING')
    if [ "$APP_INFO" == "RUNNING" ]; then
      sleep 1
      echo "waiting for result..."
    else
      set -x
      yarn logs -applicationId $APP_ID 2>&1 | grep "\[RECOVER_CDC\]"
      set +x
      break
    fi
  done
}

function flink_trans_state() {
  local chk_path=$1
  local chk_new_path=$2
  TASK_NAME="flink_trans_state"

  command="flink run-application -t yarn-application -Dclient.timeout=600s -Dparallelism.default=1 -Dtaskmanager.numberOfTaskSlots=1 \
   -Dtaskmanager.memory.process.size=1gb -Djobmanager.memory.process.size=1gb -Dtaskmanager.memory.managed.fraction=0.1 \
   -Dyarn.application.name=$TASK_NAME -c lakepump.cdc.FixState $jar_file_name $chk_path $chk_new_path"
  echo -e "\033[30;44m[read gtid]\033[0m \033[34;4m${command}\033[0m"
  APP_ID=$($command | grep "Submitted application" | awk -F" " '{print $9}')
  echo "yarn logs -applicationId $APP_ID | less"
}

function cdc2kafka() {
  flink run-application -t yarn-application -Dclient.timeout=600s -Dparallelism.default=1 -Dtaskmanager.numberOfTaskSlots=1 \
    -Dtaskmanager.memory.process.size=1gb -Djobmanager.memory.process.size=1gb -Dtaskmanager.memory.managed.fraction=0.1 \
    -Dyarn.application.name=cdc_demo -c lakepump.demo.MysqlCDCDemo $jar_file_name
}
# kafka-console-consumer.sh --bootstrap-server master-node:9092 --from-beginning --topic cdctest

function kafka2hudi() {
  # hadoop fs -rm -r /tmp/cdc_order_hudi
  flink run-application -t yarn-application -Dclient.timeout=600s -Dparallelism.default=1 -Dtaskmanager.numberOfTaskSlots=1 \
    -Dtaskmanager.memory.process.size=1gb -Djobmanager.memory.process.size=1gb -Dtaskmanager.memory.managed.fraction=0.1 \
    -Dyarn.application.name=hudi_demo -c lakepump.demo.HudiDemo $jar_file_name
}

######## hudi compaction
function hudiCompact() {
  flink run-application -t yarn-application -Dclient.timeout=600s \
    -Dparallelism.default=1 -Dtaskmanager.numberOfTaskSlots=1 \
    -Dtaskmanager.memory.process.size=1gb -Djobmanager.memory.process.size=1gb \
    -Dtaskmanager.memory.managed.fraction=0.1 -Dclassloader.check-leaked-classloader=false \
    -Dyarn.application.name=hudi_demo_compaction -c org.apache.hudi.sink.compact.HoodieFlinkCompactor \
    flink/lib/hudi-flink1.17-bundle-mod-0.14.0.jar --path hdfs://master-node:50070/tmp/cdc_order_hudi
}

######## cdc2hudi
function cdc2hudi() {
    flink run-application -t yarn-application \
      -Dclassloader.check-leaked-classloader=false -Dstate.checkpoints.num-retained=5 \
      -Dclient.timeout=600s -Dtaskmanager.slot.timeout=300s -Dakka.ask.timeout=300s \
      -Dyarn.application.name=cdc2hudi -Dyarn.application.queue=default \
      -Dtaskmanager.numberOfTaskSlots=1 -Dparallelism.default=1 \
      -Dtaskmanager.memory.process.size=1gb -Djobmanager.memory.process.size=1gb \
      -Dtaskmanager.memory.managed.fraction=0.1 -Dtaskmanager.memory.network.fraction=0.1 \
      -c lakepump.hudi.MysqlCDC2HudiV2 flink_work-1.5.jar \
      --db.instance mysql_hudi --sharding 0 --server.id 5611-5612 \
      --db.tables test_db.cdc_order --buckets 2
}

######## modify state
function transState() {
  # --path hdfs://master-node:50070/tmp/checkpoints/13008ee00bc5d8a8643e6dd221facbc0/chk-237
  command="flink run-application -t yarn-application \
    -Dclassloader.check-leaked-classloader=false -Dstate.checkpoints.num-retained=5 \
    -Dclient.timeout=600s -Dtaskmanager.slot.timeout=300s -Dakka.ask.timeout=300s \
    -Dyarn.application.name=cdcTransState -Dyarn.application.queue=default \
    -Dtaskmanager.numberOfTaskSlots=1 -Dparallelism.default=1 \
    -Dtaskmanager.memory.process.size=1gb -Djobmanager.memory.process.size=1gb \
    -Dtaskmanager.memory.managed.fraction=0.1 -Dtaskmanager.memory.network.fraction=0.1 \
    -c lakepump.cdc.TransMetaState flink_work-1.5.jar $@"
  echo -e "\033[30;42m[startup]\033[0m \033[32;4m${command}\033[0m"
  $command
}

######## hudi query
function query_hudi() {
  spark-sql \
    --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
    --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
    --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
    --conf 'spark.sql.legacy.parquet.datetimeRebaseModeInRead=CORRECTED' \
    --conf 'spark.sql.metadataCacheTTLSeconds=1'
}