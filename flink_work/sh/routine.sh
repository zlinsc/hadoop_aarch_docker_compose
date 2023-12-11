#!/bin/bash
### conf
HADOOP_CLASSPATH=$(hadoop classpath)
export HADOOP_CLASSPATH

source /home/ads/cdc/killer.sh

flink_bin_dir=/home/ads/cdc/flink-1.17.0/bin
#kinit -kt /etc/security/keytabs/ads.keytab ads/exec-093.nm.ctdcp.com@BIGDATA.CHINATELECOM.CN
kafka_kerberos_conf="-Dsecurity.kerberos.login.use-ticket-cache=false -Dsecurity.kerberos.login.contexts=Client,KafkaClient
                     -Dsecurity.kerberos.login.principal=ads/hdp-tmp007.nm.ctdcp.com@BIGDATA.CHINATELECOM.CN
                     -Dsecurity.kerberos.login.keytab=/etc/security/keytabs/ads.keytab"
flink_default_conf="-Dyarn.provided.lib.dirs=hdfs://ctyunns/user/ads/flink/lib
                    -Dclient.timeout=600s -Dtaskmanager.slot.timeout=300s -Dakka.ask.timeout=300s
                    -Djobmanager.archive.fs.dir=hdfs://ctyunns/flink-history/realjob
                    -Dhistoryserver.archive.fs.dir=hdfs://ctyunns/flink-history/realjob
                    -Dmetrics.reporter.promgateway.factory.class=org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporterFactory
                    -Dmetrics.reporter.promgateway.jobName=dcp_flink_job
                    -Dmetrics.reporter.promgateway.randomJobNameSuffix=true
                    -Dmetrics.reporter.promgateway.deleteOnShutdown=false
                    -Dmetrics.reporter.promgateway.interval=60s
                    -Dmetrics.reporter.promgateway.host=10.30.4.128
                    -Dmetrics.reporter.promgateway.port=9091"

yarn_app_list_path="./yarn_app_list.txt"

b_print_duration=0

### init
yarn application -list -appStates ALL > $yarn_app_list_path

### 配置包含如下内容：是否从最近检查点恢复启动（1/0）、库名、表名、serverid范围、yarn队列名、初始化并行度、TM内存配置
### 注意：完全重启时第一个参数必须设为0；并行度不能大于serverid个数，serverid保证数据库实例范围内唯一；并行度越高，初始化速度越快也越占资源；调大内存可以解决节点失联问题
function routine_inner() {
  local chunk_default_size=8 # db instance default num
  local restore_checkpoint_switch=$1
  local db=$2
  local table=$3
  local serverid=$4
  local yarn_queue=$5
  local parallel=$6
  local tm_mem=$7

  for ((i = 0; i < $chunk_default_size; i++)); do
    local db_i_table="${db}.${i}.${table}"
    local app_name="p#${db_i_table/_\[0-9\]\{6\}/}"

    local line_select=$(cat $yarn_app_list_path | awk '{print $2}' | sed 's/\[/\\[/g;s/\]/\\]/g' | grep -n "^$app_name$" | cut -d: -f1 | sed 's/$/p/' | tr '\n' ';')
    local line_cnt=0
    if [ -n "$line_select" ]; then
      line_cnt=$(sed -n "$line_select" $yarn_app_list_path | awk '{print $7}' | grep -E -v 'FINISHED|FAILED|KILLED' | wc -l)
    fi

    if [ $line_cnt -gt 1 ]; then
      echo "app count using $app_name is more than 1 !!!"
      killer_without_hint "^$app_name$"
      restore_checkpoint_switch=0
      line_cnt=0
    fi

    if [ $line_cnt -eq 1 ]; then
#       echo "app using $app_name is normal"
      local run_line_select=$(sed -n "$line_select" $yarn_app_list_path | awk '{print $7}' | grep -E -n -v 'FINISHED|FAILED|KILLED' | cut -d: -f1 | sed 's/$/p/' | tr '\n' ';')
      local app_id=$(sed -n "$line_select" $yarn_app_list_path | sed -n $run_line_select | awk '{print $1}')
      binlog_split_flag=$(yarn logs -applicationId $app_id | grep 'The enumerator assigns split MySqlBinlogSplit' | wc -l)
      if [ $b_print_duration -eq 1 ]; then
        ### timestamp (second)
        local curr_ts=$(date +"%s")
        local job_start_ts_str=$(yarn logs -applicationId $app_id | grep 'Submitting Job with JobId' | awk -F" " '{print $1,$2}')
        local job_start_ts_diff=0
        if [ -n "$job_start_ts_str" ]; then
          job_start_ts=$(date -d "$job_start_ts_str" +"%s")
          job_start_ts_diff=$((curr_ts-job_start_ts))
        fi
        local hours=$((job_start_ts_diff / 3600))
        local minutes=$(( (job_start_ts_diff % 3600) / 60 ))
        local seconds=$((job_start_ts_diff % 60))
        ### print message
        if [ $binlog_split_flag -eq 0 ]; then
          echo "app $app_id named as $app_name is in snapshot phase, elapsed time from the beginning is ${hours}h ${minutes}m ${seconds}s."
        else
          local job_split_ts_diff=0
          local job_split_ts_str=$(yarn logs -applicationId $app_id | grep 'The enumerator assigns split MySqlBinlogSplit' | awk -F" " '{print $1,$2}')
          if [ -n "$job_split_ts_str" ]; then
            job_split_ts=$(date -d "$job_split_ts_str" +"%s")
            job_split_ts_diff=$((job_split_ts-job_start_ts_diff))
          fi
          local split_hours=$((job_split_ts_diff / 3600))
          local split_minutes=$(( (job_split_ts_diff % 3600) / 60 ))
          local split_seconds=$((job_split_ts_diff % 60))
          echo "app $app_id named as $app_name is in split phase, elapsed time from the beginning is ${hours}h ${minutes}m ${seconds}s, snapshot duration is ${split_hours}h ${split_minutes}m ${split_seconds}s."
        fi
      else
        if [ $binlog_split_flag -eq 0 ]; then
          echo "app $app_id named as $app_name is in snapshot phase."
        else
          echo "app $app_id named as $app_name is in split phase."
        fi
      fi
    else
      echo "need to start up new app with name $app_name"
      local checkpoint_conf=""
      if [ $restore_checkpoint_switch -eq 1 ]; then
        ### get checkpoint
        local fin_line_select=$(sed -n "$line_select" $yarn_app_list_path | awk '{print $7}' | grep -E -n 'FINISHED|FAILED|KILLED' | cut -d: -f1 | sed 's/$/p/' | tr '\n' ';')
        local app_id_rows=$(sed -n "$line_select" $yarn_app_list_path | sed -n $fin_line_select | awk '{print $1}')
        local max_finish_time="0"
        local last_app_id
        while read app_id; do
          local finish_time=$(yarn application -status $app_id | grep 'Finish-Time' | awk -F": " '{print $2}')
          if ((finish_time > max_finish_time)); then
            max_finish_time=$finish_time
            last_app_id=$app_id
          fi
        done <<< "$app_id_rows"
        local flink_job_id=$(yarn logs -applicationId $last_app_id | grep 'Submitting Job with JobId' | awk -F"=" '{print $2}' | sed 's/.$//')
        local chk_path=$(hadoop fs -ls -t hdfs://ctyunns/user/ads/flink/checkpoints/$flink_job_id | grep -v '^Found' | awk -F"$flink_job_id/" '{print $2}' | head -1)
        if [ -n "$chk_path" ]; then
          checkpoint_conf="-s hdfs://ctyunns/user/ads/flink/checkpoints/$flink_job_id/$chk_path "
        fi
      fi
      ### start up
      set -x
      $flink_bin_dir/flink run-application -t yarn-application $kafka_kerberos_conf $flink_default_conf \
        -Dyarn.application.name=$app_name -Dyarn.application.queue=$yarn_queue $checkpoint_conf\
        -Djobmanager.memory.process.size=1gb -Dtaskmanager.numberOfTaskSlots=1 -Dparallelism.default=$parallel\
        -Dtaskmanager.memory.process.size=$tm_mem -Dtaskmanager.memory.managed.fraction=0.1 \
        -c MysqlCDCProducer flink_work-1.1.jar $db_i_table $serverid
      set +x
    fi
  done
}

function routine_with_checkpoint() {
  routine_inner 1 "$@"
}

function routine_without_checkpoint() {
  routine_inner 0 "$@"
}
