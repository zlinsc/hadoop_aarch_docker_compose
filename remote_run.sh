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

appid=$(get_app_id "cdc_demo")
if [ -n "$appid" ]; then
  yarn application -kill $appid
fi
flink run-application -t yarn-application -Dclient.timeout=600s -Dparallelism.default=1 -Dtaskmanager.numberOfTaskSlots=1 \
  -Dtaskmanager.memory.process.size=1gb -Djobmanager.memory.process.size=1gb -Dtaskmanager.memory.managed.fraction=0.1 \
  -Dyarn.application.name=cdc_demo -c demo.MysqlCDCDemo flink_work-1.2.jar

hadoop fs -rm -r /tmp/cdc_order_hudi

appid=$(get_app_id "hudi_demo")
if [ -n "$appid" ]; then
  yarn application -kill $appid
fi
# -Dclassloader.check-leaked-classloader=false \
flink run-application -t yarn-application -Dclient.timeout=600s -Dparallelism.default=1 -Dtaskmanager.numberOfTaskSlots=1 \
  -Dtaskmanager.memory.process.size=1gb -Djobmanager.memory.process.size=1gb -Dtaskmanager.memory.managed.fraction=0.1 \
  -Dyarn.application.name=hudi_demo -c demo.HudiDemo flink_work-1.2.jar

# appid=$(get_app_id "flinksql")
# if [ -n "$appid" ]; then
#   echo "stop" | /home/flink/bin/yarn-session.sh -id $appid
# fi
# yarn-session.sh --detached -Dyarn.application.name=flinksql -Dclient.timeout=600s \
#     -Dparallelism.default=1 -Dtaskmanager.numberOfTaskSlots=1 -Dtaskmanager.memory.process.size=1gb \
#     -Djobmanager.memory.process.size=1gb -Dtaskmanager.memory.managed.fraction=0.1
# sql-client.sh embedded -i /home/init_flink.sql -s yarn-session
