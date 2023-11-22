#!/bin/bash
function flink_read_state() {
    TASK_NAME="flink_read_state"

    export HADOOP_CLASSPATH=`hadoop classpath`
    ./flink-1.17.0/bin/flink run-application -t yarn-application -Dyarn.provided.lib.dirs=hdfs://ctyunns/user/ads/flink/lib \
     -Dsecurity.kerberos.login.use-ticket-cache=true -Dsecurity.kerberos.login.contexts=Client,KafkaClient \
     -Dsecurity.kerberos.login.principal=ads/hdp-tmp007.nm.ctdcp.com@BIGDATA.CHINATELECOM.CN \
     -Dsecurity.kerberos.login.keytab=/etc/security/keytabs/ads.keytab \
     -Dyarn.application.queue=ads -Dclient.timeout=600s \
     -Djobmanager.rpc.num-task-slots=1 -Djobmanager.cpus=1 -Djobmanager.memory.process.size=1gb \
     -Dparallelism.default=2 -Dtaskmanager.numberOfTaskSlots=1 -Dtaskmanager.memory.process.size=4gb \
     -Dyarn.application.name=$TASK_NAME \
     -c tools.RWState flink_work-1.1.jar $1

    while true; do
      APP_INFO=$(yarn application -list | grep "$TASK_NAME")
      if [ -n "$APP_INFO" ]; then
        APP_ID=$(echo "$APP_INFO" | awk '{print $1}')
        sleep 2
        echo "waiting for result..."
      else
        echo "yarn logs -applicationId $APP_ID 2>&1 | grep \"[RECOVER_CDC]\""
        yarn logs -applicationId $APP_ID 2>&1 | grep "\[RECOVER_CDC\]"
        break
      fi
    done
}
flink_read_state "hdfs://ctyunns/user/ads/flink/checkpoints/eaf6b1e68063c6def145e4389298fce7/chk-421"