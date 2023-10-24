#!/bin/bash
HADOOP_CLASSPATH=$(hadoop classpath)
export HADOOP_CLASSPATH

yarn_resource_web=localhost:8088
flink_bin_dir=./flink-1.17.0/bin/
kafka_kerberos_conf="-Dsecurity.kerberos.login.use-ticket-cache=true -Dsecurity.kerberos.login.contexts=Client,KafkaClient
                     -Dsecurity.kerberos.login.principal=ads/hdp-tmp007.nm.ctdcp.com@BIGDATA.CHINATELECOM.CN
                     -Dsecurity.kerberos.login.keytab=/etc/security/keytabs/ads.keytab"
flink_default_conf="-Dyarn.provided.lib.dirs=hdfs://ctyunns/user/ads/flink/lib -Dclient.timeout=600s
                    -Djobmanager.archive.fs.dir=hdfs://ctyunns/flink-history/realjob
                    -Dhistoryserver.archive.fs.dir=hdfs://ctyunns/flink-history/realjob"

function get_appid_using_name() {
  if [ $# -ne 1 ]; then
    echo "Usage: $0 <app_name>"
    exit 1
  fi

  local APP_NAME=$1
  local APP_ID

  for ((i = 0; i < 1; i++)); do
    APP_ID=$(yarn application -list | grep "$APP_NAME" | awk '{print $1}')
    echo "$APP_ID"
  done
}

function kill_appid() {
  if [ $# -ne 1 ]; then
    echo "Usage: $0 <app_id>"
    exit 1
  fi

  local APP_ID=$1

  for ((i = 0; i < 1; i++)); do
    if [ -n "$APP_ID" ]; then
      echo "Killing app $APP_ID with name $APP_NAME"
      yarn application -kill "$APP_ID"
    else
      echo "No application found with name $APP_NAME"
    fi
  done
}

function get_app_state() {
  if [ $# -ne 1 ]; then
    echo "Usage: $0 <app_name>"
    exit 1
  fi

  local APP_NAME=$1
  local STATE

  for ((i = 0; i < 1; i++)); do
    STATE=$(yarn application -list | grep "$APP_NAME" | awk '{print $7}')
    echo "$STATE"
  done
}

function is_app_status_normal() {
  if [ $# -ne 1 ]; then
    echo "Usage: $0 <app_name>"
    exit 1
  fi

  local APP_NAME=$1
  local RUN_FLAG="N"
  local STATE

  while true; do
    STATE=$(get_app_state "$APP_NAME")
    if [ "$STATE" = "FINISHED" ] || { [ "$STATE" = "RUNNING" ] && [ "$RUN_FLAG" = "Y" ]; }; then
      echo "status is normal (finished or running for 10s)"
      return 0
    elif [ "$STATE" = "FAILED" ] || [ "$STATE" = "KILLED" ]; then
      echo "status is abnormal (failed or killed)"
      return 1
    elif [ "$STATE" = "RUNNING" ] && [ "$RUN_FLAG" = "N" ]; then
      echo "status is running...(wait for 10s)"
      RUN_FLAG="Y"
      sleep 10
    else
      echo "waiting for result..."
      sleep 2
    fi
  done
}

function cdc_startup_app() {
  if [ $# -ne 3 ]; then
    echo "Usage: $0 <app_name> <yarn_queue> <taskmanager_mem>"
    exit 1
  fi

  local APP_NAME=$1
  local QUEUE=$2
  local TM_MEM=$3
  local ret

  echo "Start up new app with name $APP_NAME"
  $flink_bin_dir/yarn-session.sh --detached $kafka_kerberos_conf $flink_default_conf \
    -Dyarn.application.name=cdc_$APP_NAME -Dyarn.application.queue=$QUEUE \
    -Djobmanager.memory.process.size=1gb -Dtaskmanager.numberOfTaskSlots=1 \
    -Dtaskmanager.memory.process.size=$TM_MEM -Dtaskmanager.memory.managed.fraction=0.1
  ret=$(is_app_status_normal $APP_NAME)
  if [ $ret -eq 1 ]; then
    exit 1
  fi
}

function cdc_shutdown_app() {
  if [ $# -ne 1 ]; then
    echo "Usage: $0 <app_name>"
    exit 1
  fi

  local APP_NAME=$1
  local APP_ID

  APP_ID=$(get_appid_using_name "$APP_NAME")
  if [ -n "$APP_ID" ]; then
    echo "Shut down app $APP_ID with name $APP_NAME"
    echo "stop" | $flink_bin_dir/yarn-session.sh -id "$APP_ID"
  else
    echo "No application found with name $APP_NAME"
  fi
}

##############################################
function cdc_startup_job() {
  if [ $# -ne 2 ]; then
    echo "Usage: $0 <app_name> <params>"
    exit 1
  fi

  local APP_NAME=$1
  local PARAMS="$2" # "cust.0.PROD_SPEC_INST_ATTR serverid gtid/6dc8b5af-1616-11ec-8f60-a4ae12fe8402:1-20083670,6de8242f-1616-11ec-94a2-a4ae12fe9796:1-700110909"
  local APP_ID

  APP_ID=$(get_appid_using_name "$APP_NAME")
  if [ -n "$APP_ID" ]; then
    echo "Start up new job in $APP_ID with name $APP_NAME"
    $flink_bin_dir/flink run -t yarn-session --detached -Dyarn.application.id=$APP_ID \
      -Dparallelism.default=1 -c MysqlCDCProducer flink_work-1.1.jar $PARAMS
  else
    echo "No application found with name $APP_NAME"
  fi
}

function cdc_shutdown_job() {
  if [ $# -ne 2 ]; then
    echo "Usage: $0 <app_name> <job_name>"
    exit 1
  fi

  local APP_NAME=$1
  local JOB_NAME=$2
  local APP_ID
  local resp
  local jobid

  APP_ID=$(get_appid_using_name "$APP_NAME")
  if [ -n "$APP_ID" ]; then
    resp=$(curl -s "http://$yarn_resource_web/proxy/$APP_ID/jobs/overview")
    jobid=$(echo "$resp" | jq -r ".jobs[] | select(.name == \"$JOB_NAME\" and .state == \"RUNNING\") | .jid")
    if [ -n "$jobid" ]; then
      echo "Killing job $jobid with name $JOB_NAME"
      $flink_bin_dir/flink cancel -t yarn-session -Dyarn.application.id=$APP_ID $jobid
    else
      echo "No job found with name $JOB_NAME"
    fi
  fi
}

function cdc_read_state() {
  if [ $# -ne 2 ]; then
    echo "Usage: $0 <yarn_queue> <chk_path>"
    exit 1
  fi

  local QUEUE=$1
  local CHK_PATH=$2
  local APP_NAME="cdc_read_state"
  local APP_INFO
  local APP_ID

  echo "Fetch flink state"
  $flink_bin_dir/flink run-application -t yarn-application $kafka_kerberos_conf $flink_default_conf \
    -Dyarn.application.name=$APP_NAME -Dyarn.application.queue=$QUEUE \
    -Djobmanager.memory.process.size=1gb -Dtaskmanager.numberOfTaskSlots=1 -Dparallelism.default=1 \
    -Dtaskmanager.memory.process.size=1gb -Dtaskmanager.memory.managed.fraction=0.1 \
    -c tools.flink.CDCStateReader flink_work-1.1.jar $CHK_PATH

  while true; do
    APP_INFO=$(yarn application -list | grep "$APP_NAME")
    if [ -n "$APP_INFO" ]; then
      APP_ID=$(echo "$APP_INFO" | awk '{print $1}')
      sleep 2
      echo "waiting for result..."
    else
      if [ -n "$APP_ID" ]; then
        echo "yarn logs -applicationId $APP_ID 2>&1 | grep \"[RECOVER_CDC]\""
        yarn logs -applicationId $APP_ID 2>&1 | grep "\[RECOVER_CDC\]"
        break
      else
        sleep 2
        echo "waiting for result..."
      fi
    fi
  done
}

function cdc_restart_job() {
  if [ $# -ne 4 ]; then
    echo "Usage: $0 <db> <table> <serverid> <yarn_queue>"
    exit 1
  fi

  local DB=$1
  local TABLE=$2
  local SERVERID=$3
  local QUEUE=$4
  local resp
  local external_path
  local rslt
  local pos

  for ((i = 0; i < 8; i++)); do
    cdc_shutdown_job $APP_NAME

    resp=$(curl -s "http://$yarn_resource_web/proxy/$APP_ID/jobs/$jobid/checkpoints")
    external_path=$(echo "$resp" | jq -r '.latest.completed.external_path')
    if [ -n "$external_path" ]; then
      echo "Read checkpoint path: $external_path"
      rslt=$(cdc_read_state $QUEUE $external_path)
      if [ -n "$rslt" ]; then
        pos=$(echo "$rslt" | awk -F'=' '{print $2}')
        local PARAMS="$DB.$i.$TABLE $SERVERID gtid/$pos"
        cdc_startup_job $PARAMS
      else
        echo "Fail to get cdc state"
      fi
    else
      echo "No checkpoint path found"
    fi
  done
}
