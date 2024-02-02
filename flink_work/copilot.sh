#!/bin/bash
################### table & bucket mapping
declare -A buckets

buckets["cust.prod_spec_inst"]="23"
buckets["cust.prod_spec_inst_attr"]="280"
buckets["cust.prod_spec_inst_label_rel"]="1"
buckets["cust.prod_spec_res_inst"]="23"
buckets["cust.resource_view_inst"]="1"
buckets["cust.resource_view_inst_attr"]="1"
buckets["order.inner_ord_offer_inst_pay_info"]="1"
buckets["order.inner_ord_offer_inst_pay_info_his"]="20"
buckets["order.inner_ord_offer_prod_inst_rel"]="1"
buckets["order.inner_ord_offer_prod_inst_rel_his"]="24"
buckets["order.inner_ord_prod_spec_inst"]="1"
buckets["order.inner_ord_prod_spec_inst_his"]="24"
buckets["order.inner_ord_prod_spec_res_inst"]="1"
buckets["order.inner_ord_prod_spec_res_inst_his"]="24"
buckets["order.master_order"]="1"
buckets["order.master_order_attr"]="1"
buckets["order.master_order_attr_his"]="93"
buckets["order.master_order_his"]="13"
buckets["order.ord_offer_inst"]="1"
buckets["order.ord_offer_inst_attr"]="1"
buckets["order.ord_offer_inst_attr_his"]="113"
buckets["order.ord_offer_inst_his"]="22"
buckets["order.ord_prod_spec_inst"]="1"
buckets["order.ord_prod_spec_inst_attr"]="1"
buckets["order.ord_prod_spec_inst_attr_his"]="432"
buckets["order.ord_prod_spec_inst_his"]="22"
buckets["order.order_attr"]="1"
buckets["order.order_attr_his"]="80"
buckets["order.order_item"]="1"
buckets["order.order_item_attr"]="1"
buckets["order.order_item_attr_his"]="1"
buckets["order.order_item_his"]="44"
buckets["order.order_meta"]="1"
buckets["order.order_meta_his"]="13"
buckets["order.order_pay_info"]="1"
buckets["order.order_pay_info_his"]="3"

buckets["order.inner_order_attr_his"]="104"

buckets["acctdb.acct_item_total_month_[0-9]{6}"]="100"
buckets["acctdb.order_info_log"]="8"
buckets["acctdb.payment"]="10"

# todo
buckets["cust.offer_inst"]="19"
buckets["cust.offer_prod_inst_rel"]="19"
buckets["cust.prod_spec_inst_acct_rel"]="19"
buckets["cust.discount_inst"]="1"
buckets["cust.offer_price_plan_inst"]="12"
buckets["cust.account_base"]="3"
buckets["cust.account_attr"]="22"
buckets["cust.offer_inst_attr"]="35"
buckets["cust.offer_inst_pay_info"]="7"

################### init
export HADOOP_CLASSPATH=`hadoop classpath`
yarn_resource_web="flink-cdc-003.nm.ctdcp.com:8088"
flink_bin_dir=/home/ads/cdc/flink-1.17.0-x/bin
jar_path=/home/ads/cdc/flink_work-1.4.jar

yarn_app_list_path="/home/ads/oper/yarn_app_list.txt"
yarn application -list > $yarn_app_list_path


################### shutdown cdc job
function get_appid_using_name() {
  local app_name=$1

  for ((i = 0; i < 1; i++)); do
    local appid=$(cat $yarn_app_list_path | grep "$app_name" | awk '{print $1}')
    echo "$appid"
  done
}

function shutdown_job() {
  local app_name=$1
  local flink_appid=$(get_appid_using_name "$app_name")

  if [ -n "$flink_appid" ]; then
    local jobid=$(hadoop fs -cat hdfs://ctyunns/user/ads/flink/jobid_cache/$app_name)
    #http://flink-cdc-003.nm.ctdcp.com:8088/proxy/application_1701850654341_0001/jobs/dd6b63cbdbc62acb90ca1582c20446b3/yarn-cancel
    local url="http://$yarn_resource_web/proxy/$flink_appid/jobs/$jobid/yarn-cancel"
    local command="curl -s $url"
    echo -e "\033[30;43m[shutdown]\033[0m \033[33;4m${command}\033[0m"
    $command
  fi
}


################### startup cdc2kafka from crm job
function run2kafka_crm() {
  local sharding_id=$1
  local appName="test_cdc2kafka_mysql_crm_sharding_$sharding_id"
  local server_id="5666-5669" #"5601-5604"

  # TODO add or delete db.table
  dbTablesStr="cust.offer_inst,cust.offer_prod_inst_rel"
#  dbTablesStr="cust.prod_spec_inst,cust.prod_spec_inst_attr,cust.prod_spec_inst_label_rel,cust.prod_spec_res_inst,cust.resource_view_inst,cust.resource_view_inst_attr,order.inner_ord_offer_inst_pay_info,order.inner_ord_offer_inst_pay_info_his,order.inner_ord_offer_prod_inst_rel,order.inner_ord_offer_prod_inst_rel_his,order.inner_ord_prod_spec_inst,order.inner_ord_prod_spec_inst_his,order.inner_ord_prod_spec_res_inst,order.inner_ord_prod_spec_res_inst_his,order.master_order,order.master_order_attr,order.master_order_attr_his,order.master_order_his,order.ord_offer_inst,order.ord_offer_inst_attr,order.ord_offer_inst_attr_his,order.ord_offer_inst_his,order.ord_prod_spec_inst,order.ord_prod_spec_inst_attr,order.ord_prod_spec_inst_attr_his,order.ord_prod_spec_inst_his,order.order_attr,order.order_attr_his,order.order_item,order.order_item_attr,order.order_item_attr_his,order.order_item_his,order.order_meta,order.order_meta_his,order.order_pay_info,order.order_pay_info_his"
  IFS=',' read -ra tbls <<< "$dbTablesStr"
  bucketStr=""
  for element in "${tbls[@]}"; do
    e=${buckets[$element]}
    if [ -n "$bucketStr" ]; then
      bucketStr+=",$e"
    else
      bucketStr="$e"
    fi
  done

  command="$flink_bin_dir/flink run-application -t yarn-application -Dyarn.provided.lib.dirs=hdfs://ctyunns/user/ads/flink/lib2 \
    -Dsecurity.kerberos.login.use-ticket-cache=false -Dsecurity.kerberos.login.contexts=Client,KafkaClient \
    -Dsecurity.kerberos.login.principal=ads/hdp-tmp007.nm.ctdcp.com@BIGDATA.CHINATELECOM.CN \
    -Dsecurity.kerberos.login.keytab=/etc/security/keytabs/ads.keytab \
    -Dclassloader.check-leaked-classloader=false -Dstate.checkpoints.num-retained=5 \
    -Dexecution.checkpointing.timeout=1800000 \
    -Djobmanager.archive.fs.dir=hdfs://ctyunns/flink-history/realjob -Dhistoryserver.archive.fs.dir=hdfs://ctyunns/flink-history/realjob \
    -Dclient.timeout=600s -Dtaskmanager.slot.timeout=300s -Dakka.ask.timeout=300s \
    -Dyarn.application.name=$appName -Dyarn.application.queue=dws \
    -Djobmanager.memory.process.size=8gb -Dtaskmanager.numberOfTaskSlots=4 -Dparallelism.default=4 \
    -Dtaskmanager.memory.process.size=16gb -Dtaskmanager.memory.managed.fraction=0.2 -Dtaskmanager.memory.network.fraction=0.1 \
    -c lakepump.kafka.MysqlCDC2Kafka $jar_path dbInstance=mysql_crm serverId=$server_id sharding=$sharding_id appName=$appName \
    dbTables=$dbTablesStr buckets=$bucketStr"
  echo -e "\033[30;42m[startup]\033[0m \033[32;4m${command}\033[0m"
  $command
}


################### startup cdc2kafka from account job
function run2kafka_account() {
  local sharding_id=$1
  local appName="cdc2kafka_mysql_account_sharding_$sharding_id"

  # TODO add or delete db.table
  dbTablesStr="acctdb.acct_item_total_month_[0-9]{6},acctdb.order_info_log,acctdb.payment"
  IFS=',' read -ra tbls <<< "$dbTablesStr"
  bucketStr=""
  for element in "${tbls[@]}"; do
    e=${buckets[$element]}
    if [ -n "$bucketStr" ]; then
      bucketStr+=",$e"
    else
      bucketStr="$e"
    fi
  done

  command="$flink_bin_dir/flink run-application -t yarn-application -Dyarn.provided.lib.dirs=hdfs://ctyunns/user/ads/flink/lib2 \
    -Dsecurity.kerberos.login.use-ticket-cache=false -Dsecurity.kerberos.login.contexts=Client,KafkaClient \
    -Dsecurity.kerberos.login.principal=ads/hdp-tmp007.nm.ctdcp.com@BIGDATA.CHINATELECOM.CN \
    -Dsecurity.kerberos.login.keytab=/etc/security/keytabs/ads.keytab \
    -Dclassloader.check-leaked-classloader=false -Dstate.checkpoints.num-retained=5 \
    -Djobmanager.archive.fs.dir=hdfs://ctyunns/flink-history/realjob -Dhistoryserver.archive.fs.dir=hdfs://ctyunns/flink-history/realjob \
    -Dclient.timeout=600s -Dtaskmanager.slot.timeout=300s -Dakka.ask.timeout=300s \
    -Dyarn.application.name=$appName -Dyarn.application.queue=dws \
    -Djobmanager.memory.process.size=4gb -Dtaskmanager.numberOfTaskSlots=4 -Dparallelism.default=4 \
    -Dtaskmanager.memory.process.size=8gb -Dtaskmanager.memory.managed.fraction=0.2 -Dtaskmanager.memory.network.fraction=0.1 \
    -c lakepump.kafka.MysqlCDC2Kafka $jar_path dbInstance=mysql_account serverId=5601-5604 sharding=$sharding_id appName=$appName \
    dbTables=$dbTablesStr buckets=$bucketStr"
  echo -e "\033[30;42m[startup]\033[0m \033[32;4m${command}\033[0m"
  $command
}


################### startup cdc2hudi from crm job
function run2hudi_crm() {
  local sharding_id=$1
  local appName="cdc2hudi_mysql_crm_sharding_$sharding_id"

  # TODO add or delete db.table
  dbTablesStr="order.inner_order_attr_his,order.master_order_attr_his,order.order_attr_his,order.order_item_his"
  IFS=',' read -ra tbls <<< "$dbTablesStr"
  bucketStr=""
  for element in "${tbls[@]}"; do
    e=${buckets[$element]}
    if [ -n "$bucketStr" ]; then
      bucketStr+=",$e"
    else
      bucketStr="$e"
    fi
  done

  command="$flink_bin_dir/flink run-application -t yarn-application -Dyarn.provided.lib.dirs=hdfs://ctyunns/user/ads/flink/lib2 \
    -Dsecurity.kerberos.login.use-ticket-cache=false -Dsecurity.kerberos.login.contexts=Client,KafkaClient \
    -Dsecurity.kerberos.login.principal=ads/hdp-tmp007.nm.ctdcp.com@BIGDATA.CHINATELECOM.CN \
    -Dsecurity.kerberos.login.keytab=/etc/security/keytabs/ads.keytab \
    -Dclassloader.check-leaked-classloader=false -Dstate.checkpoints.num-retained=5 \
    -Djobmanager.archive.fs.dir=hdfs://ctyunns/flink-history/realjob -Dhistoryserver.archive.fs.dir=hdfs://ctyunns/flink-history/realjob \
    -Dclient.timeout=600s -Dtaskmanager.slot.timeout=300s -Dakka.ask.timeout=300s \
    -Dyarn.application.name=$appName -Dyarn.application.queue=dws \
    -Djobmanager.memory.process.size=8gb -Dtaskmanager.numberOfTaskSlots=4 -Dparallelism.default=4 \
    -Dtaskmanager.memory.process.size=32gb -Dtaskmanager.memory.managed.fraction=0.2 -Dtaskmanager.memory.network.fraction=0.1 \
    -c lakepump.hudi.MysqlCDC2Hudi $jar_path dbInstance=mysql_crm serverId=5611-5614 sharding=$sharding_id appName=$appName \
    dbTables=$dbTablesStr buckets=$bucketStr"
  echo -e "\033[30;42m[startup]\033[0m \033[32;4m${command}\033[0m"
  $command
}


################### shutdown hudi compact job
function shutdown_compact_job() {
  local compact_hudi_appid=$(get_appid_using_name "compact_hudi_tables")
  if [ -n "$compact_hudi_appid" ]; then
    local command="echo 'stop' | $flink_bin_dir/yarn-session.sh -id $compact_hudi_appid"
    echo -e "\033[30;43m[shutdown]\033[0m \033[33;4m${command}\033[0m"
    $command
  fi
}


################### startup hudi compact job
function hudi_compact() {
  local APP_ID=$(get_appid_using_name "compact_hudi_tables")

  keys=("${!buckets[@]}")
  dbTablesStr=$(IFS=','; echo "${keys[*]}")
  IFS=',' read -ra tbls <<< "$dbTablesStr"
  bucketStr=""
  for element in "${tbls[@]}"; do
    e=${buckets[$element]}
    if [ -n "$bucketStr" ]; then
      bucketStr+=",$e"
    else
      bucketStr="$e"
    fi
  done

  if [ -z "$APP_ID" ]; then
    command="$flink_bin_dir/flink run-application -t yarn-application -Dyarn.provided.lib.dirs=hdfs://ctyunns/user/ads/flink/lib2 \
      -Dsecurity.kerberos.login.use-ticket-cache=false -Dsecurity.kerberos.login.contexts=Client,KafkaClient \
      -Dsecurity.kerberos.login.principal=ads/hdp-tmp007.nm.ctdcp.com@BIGDATA.CHINATELECOM.CN \
      -Dsecurity.kerberos.login.keytab=/etc/security/keytabs/ads.keytab \
      -Dclassloader.check-leaked-classloader=false \
      -Dclient.timeout=600s -Dtaskmanager.slot.timeout=300s -Dakka.ask.timeout=300s \
      -Dyarn.application.name=compact_hudi_tables -Dyarn.application.queue=ads \
      -Djobmanager.memory.process.size=4gb -Dtaskmanager.numberOfTaskSlots=4 -Dparallelism.default=4 \
      -Dtaskmanager.memory.process.size=16gb -Dtaskmanager.memory.managed.fraction=0.1 -Dtaskmanager.memory.network.fraction=0.1 \
      -c lakepump.hudi.TimingCompactor $jar_path \
      dbTables=$dbTablesStr buckets=$bucketStr"
    echo -e "\033[30;42m[startup]\033[0m \033[32;4m${command}\033[0m"
    $command
  fi
}


################### auto restart
function auto_invoke() {
  local func_name="$1"
  local app_name="$2"

  line_cnt=$(cat $yarn_app_list_path | awk '{print $2}' | grep -n "^$app_name$" | wc -l)
  if [ $line_cnt -eq 0 ]; then
    echo "$app_name is stop"
    $func_name
  else
    echo "$app_name is running"
  fi
}

function auto_restart() {
  parts=(0 1 2 3 4 5 6 7)
  for i in "${parts[@]}"; do
    auto_invoke "run2kafka_crm $i" "cdc2kafka_mysql_crm_sharding_$i"
    auto_invoke "run2kafka_account $i" "cdc2kafka_mysql_account_sharding_$i"
    auto_invoke "run2hudi_crm $i" "cdc2hudi_mysql_crm_sharding_$i"
  done
  auto_invoke hudi_compact "compact_hudi_tables"
}


################### read gtid
function flink_read_state() {
  local chk_path=$1
  TASK_NAME="flink_read_state"

  command="$flink_bin_dir/flink run-application -t yarn-application -Dyarn.provided.lib.dirs=hdfs://ctyunns/user/ads/flink/lib2 \
   -Dsecurity.kerberos.login.use-ticket-cache=true -Dsecurity.kerberos.login.contexts=Client,KafkaClient \
   -Dsecurity.kerberos.login.principal=ads/hdp-tmp007.nm.ctdcp.com@BIGDATA.CHINATELECOM.CN \
   -Dsecurity.kerberos.login.keytab=/etc/security/keytabs/ads.keytab \
   -Dyarn.application.name=$TASK_NAME -Dyarn.application.queue=ads -Dclient.timeout=600s \
   -Djobmanager.memory.process.size=1gb -Dtaskmanager.memory.process.size=4gb \
   -Dtaskmanager.numberOfTaskSlots=1 -Dparallelism.default=2 \
   -c lakepump.cdc.CDCStateReader $jar_path $chk_path"
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


################### exception state
function grep_error_log() {
  local app_name="$1"
  local app_id=$(get_appid_using_name "$app_name")

  line_cnt=$(yarn logs -applicationId $app_id | grep 'The enumerator received invalid request meta group id' | wc -l)
  if [ $line_cnt -gt 0 ]; then
    echo "$app_name is running in an unknown state"
  fi
}

function exception_state_detector() {
  parts=(0 1 2 3 4 5 6 7)
  for i in "${parts[@]}"; do
    grep_error_log "cdc2kafka_mysql_crm_sharding_$i"
    grep_error_log "cdc2kafka_mysql_account_sharding_$i"
    grep_error_log "cdc2hudi_mysql_crm_sharding_$i"
  done
}


######################################
# TODO >>>>>>>>>>>>>>
#shutdown_compact_job
#my_list=(0 1 2 3 4 5 6 7)
#for i in "${my_list[@]}"; do
# shutdown_job cdc2kafka_mysql_crm_sharding_$i
# shutdown_job cdc2kafka_mysql_account_sharding_$i
# shutdown_job cdc2hudi_mysql_crm_sharding_$i
#
# run2kafka_crm $i
# run2kafka_account $i
# run2hudi_crm $i
#done
#hudi_compact
######################################

#flink_read_state "hdfs://ctyunns/user/ads/flink/ckp/eaf6b1e68063c6def145e4389298fce7/chk-421"