#!/bin/bash
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

######## cdc2kafka
#appid=$(get_app_id "cdc_demo")
#if [ -n "$appid" ]; then
#  yarn application -kill $appid
#fi
flink run-application -t yarn-application -Dclient.timeout=600s -Dparallelism.default=1 -Dtaskmanager.numberOfTaskSlots=1 \
   -Dtaskmanager.memory.process.size=1gb -Djobmanager.memory.process.size=1gb -Dtaskmanager.memory.managed.fraction=0.1 \
   -Dyarn.application.name=cdc_demo -c lakepump.demo.MysqlCDCDemo flink_work-1.3.jar

# kafka-console-consumer.sh --bootstrap-server master-node:9092 --from-beginning --topic cdctest

######## kafka2hudi
hadoop fs -rm -r /tmp/cdc_order_hudi

#appid=$(get_app_id "hudi_demo")
#if [ -n "$appid" ]; then
#  yarn application -kill $appid
#fi
flink run-application -t yarn-application -Dclient.timeout=600s -Dparallelism.default=1 -Dtaskmanager.numberOfTaskSlots=1 \
  -Dtaskmanager.memory.process.size=1gb -Djobmanager.memory.process.size=1gb -Dtaskmanager.memory.managed.fraction=0.1 \
  -Dyarn.application.name=hudi_demo -c lakepump.demo.HudiDemo flink_work-1.3.jar

######## cdc2hudi
#flink run-application -t yarn-application \
#  -Dclassloader.check-leaked-classloader=false \
#  -Dclient.timeout=600s -Dtaskmanager.slot.timeout=300s -Dakka.ask.timeout=300s \
#  -Dyarn.application.name=cdc2hudi -Dyarn.application.queue=default \
#  -Djobmanager.memory.process.size=1gb -Dtaskmanager.numberOfTaskSlots=1 -Dparallelism.default=1 \
#  -Dtaskmanager.memory.process.size=1gb -Dtaskmanager.memory.managed.fraction=0.1 -Dtaskmanager.memory.network.fraction=0.1 \
#  -c lakepump.demo.MysqlCDC2HudiDemo flink_work-1.3.jar dbInstance=mysql_hudi serverId=5611-5614 sharding=0 appName=cdc2hudi \
#  dbTables=test_db.cdc_order buckets=2

######## hudi compaction
# flink run-application -t yarn-application -Dclient.timeout=600s -Dparallelism.default=1 -Dtaskmanager.numberOfTaskSlots=1 \
#   -Dtaskmanager.memory.process.size=1gb -Djobmanager.memory.process.size=1gb -Dtaskmanager.memory.managed.fraction=0.1 -Dclassloader.check-leaked-classloader=false \
#   -Dyarn.application.name=hudi_demo_compaction -c org.apache.hudi.sink.compact.HoodieFlinkCompactor flink/lib/hudi-flink1.17-bundle-mod-0.14.0.jar --path hdfs://master-node:50070/tmp/cdc_order_hudi

######## spark sql
# spark-sql \
# --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
# --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
# --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
# --conf 'spark.sql.legacy.parquet.datetimeRebaseModeInRead=CORRECTED' \
# --conf 'spark.sql.metadataCacheTTLSeconds=1' 