#!/bin/bash
export HADOOP_CLASSPATH=`hadoop classpath`
yarn_resource_web="flink-cdc-003.nm.ctdcp.com:8088"
flink_bin_dir=/home/ads/cdc/flink-1.17.0-x/bin
jar_path=/home/ads/cdc/flink_work-1.3.jar

if [ $# -eq 1 ]; then
  auto_switch="Y"
fi

################### backup checkpoint
function backup_jobid_cache() {
  local APP_NAME=$1
  hadoop fs -get -f hdfs://ctyunns/user/ads/flink/jobid_cache/$APP_NAME
}

################### rm jobid_cache
function rm_jobid_cache() {
  local APP_NAME=$1
  hadoop fs -rm hdfs://ctyunns/user/ads/flink/jobid_cache/$APP_NAME
}

################### shutdown job
function get_appid_using_name() {
  local APP_NAME=$1
  for ((i = 0; i < 1; i++)); do
    local APP_ID=$(yarn application -list | grep "$APP_NAME" | awk '{print $1}')
    echo "$APP_ID"
  done
}

function shutdown_and_clean() {
  local APP_ID=$(get_appid_using_name "compact_hudi_tables")
  if [ -n "$APP_ID" ]; then
    set -x
    yarn app -kill $APP_ID
    set +x
  fi

  local APP_NAME=$1
  backup_jobid_cache $APP_NAME

  local APP_ID=$(get_appid_using_name "$APP_NAME")
  if [ -n "$APP_ID" ]; then
    local JOB_ID=$(hadoop fs -cat hdfs://ctyunns/user/ads/flink/jobid_cache/$APP_NAME)
    #http://flink-cdc-003.nm.ctdcp.com:8088/proxy/application_1701850654341_0001/jobs/dd6b63cbdbc62acb90ca1582c20446b3/yarn-cancel
    local url="http://$yarn_resource_web/proxy/$APP_ID/jobs/$JOB_ID/yarn-cancel"
    echo $url
    curl -s $url
  fi

  # TODO remind here
  rm_jobid_cache $APP_NAME
  if [ $# -eq 2 ]; then
    set -x
    hadoop fs -rm -r hdfs://ctyunns/user/ads/hudi/hudi_order/*_$2
    hadoop fs -rm -r hdfs://ctyunns/user/ads/hudi/hudi_cust/*_$2
    set +x

    roTables=$(hive -e "use hudi_order;show tables like '*_$2_ro';" | awk '{print $1}')
    rtTables=$(hive -e "use hudi_order;show tables like '*_$2_rt';" | awk '{print $1}')
    dropCommands="use hudi_order;"
    for table in $roTables; do
      command="drop table $table;"
      dropCommands+="$command"
    done
    for table in $rtTables; do
      command="drop table $table;"
      dropCommands+="$command"
    done
    set -x
    hive -e "$dropCommands"
    set +x

    roTables=$(hive -e "use hudi_cust;show tables like '*_$2_ro';" | awk '{print $1}')
    rtTables=$(hive -e "use hudi_cust;show tables like '*_$2_rt';" | awk '{print $1}')
    dropCommands="use hudi_cust;"
    for table in $roTables; do
      command="drop table $table;"
      dropCommands+="$command"
    done
    for table in $rtTables; do
      command="drop table $table;"
      dropCommands+="$command"
    done
    set -x
    hive -e "$dropCommands"
    set +x
  fi
}

################### kafka
function run2kafka_crm() {
  local appName="cdc2kafka_mysql_crm_sharding_$1"
  $flink_bin_dir/flink run-application -t yarn-application -Dyarn.provided.lib.dirs=hdfs://ctyunns/user/ads/flink/lib2 \
    -Dsecurity.kerberos.login.use-ticket-cache=false -Dsecurity.kerberos.login.contexts=Client,KafkaClient \
    -Dsecurity.kerberos.login.principal=ads/hdp-tmp007.nm.ctdcp.com@BIGDATA.CHINATELECOM.CN \
    -Dsecurity.kerberos.login.keytab=/etc/security/keytabs/ads.keytab \
    -Dclassloader.check-leaked-classloader=false \
    -Djobmanager.archive.fs.dir=hdfs://ctyunns/flink-history/realjob -Dhistoryserver.archive.fs.dir=hdfs://ctyunns/flink-history/realjob \
    -Dclient.timeout=600s -Dtaskmanager.slot.timeout=300s -Dakka.ask.timeout=300s \
    -Dyarn.application.name=$appName -Dyarn.application.queue=dws \
    -Djobmanager.memory.process.size=8gb -Dtaskmanager.numberOfTaskSlots=4 -Dparallelism.default=4 \
    -Dtaskmanager.memory.process.size=16gb -Dtaskmanager.memory.managed.fraction=0.2 -Dtaskmanager.memory.network.fraction=0.1 \
    -c lakepump.kafka.MysqlCDC2Kafka $jar_path dbInstance=mysql_crm serverId=5601-5604 sharding=$1 appName=$appName \
    dbTables=cust.prod_spec_inst,cust.prod_spec_inst_attr,cust.prod_spec_inst_label_rel,cust.prod_spec_res_inst,cust.resource_view_inst,cust.resource_view_inst_attr,order.inner_ord_offer_inst_pay_info,order.inner_ord_offer_inst_pay_info_his,order.inner_ord_offer_prod_inst_rel,order.inner_ord_offer_prod_inst_rel_his,order.inner_ord_prod_spec_inst,order.inner_ord_prod_spec_inst_his,order.inner_ord_prod_spec_res_inst,order.inner_ord_prod_spec_res_inst_his,order.master_order,order.master_order_attr,order.master_order_attr_his,order.master_order_his,order.ord_offer_inst,order.ord_offer_inst_attr,order.ord_offer_inst_attr_his,order.ord_offer_inst_his,order.ord_prod_spec_inst,order.ord_prod_spec_inst_attr,order.ord_prod_spec_inst_attr_his,order.ord_prod_spec_inst_his,order.order_attr,order.order_attr_his,order.order_item,order.order_item_attr,order.order_item_attr_his,order.order_item_his,order.order_meta,order.order_meta_his,order.order_pay_info,order.order_pay_info_his \
    buckets=23,280,1,23,1,1,1,20,1,24,1,24,1,24,1,1,93,13,1,1,113,22,1,1,432,22,1,80,1,1,1,44,1,13,1,3
}

function run2kafka_account() {
  local appName="cdc2kafka_mysql_account_sharding_$1"
  $flink_bin_dir/flink run-application -t yarn-application -Dyarn.provided.lib.dirs=hdfs://ctyunns/user/ads/flink/lib2 \
    -Dsecurity.kerberos.login.use-ticket-cache=false -Dsecurity.kerberos.login.contexts=Client,KafkaClient \
    -Dsecurity.kerberos.login.principal=ads/hdp-tmp007.nm.ctdcp.com@BIGDATA.CHINATELECOM.CN \
    -Dsecurity.kerberos.login.keytab=/etc/security/keytabs/ads.keytab \
    -Dclassloader.check-leaked-classloader=false \
    -Djobmanager.archive.fs.dir=hdfs://ctyunns/flink-history/realjob -Dhistoryserver.archive.fs.dir=hdfs://ctyunns/flink-history/realjob \
    -Dclient.timeout=600s -Dtaskmanager.slot.timeout=300s -Dakka.ask.timeout=300s \
    -Dyarn.application.name=$appName -Dyarn.application.queue=dws \
    -Djobmanager.memory.process.size=4gb -Dtaskmanager.numberOfTaskSlots=4 -Dparallelism.default=4 \
    -Dtaskmanager.memory.process.size=8gb -Dtaskmanager.memory.managed.fraction=0.2 -Dtaskmanager.memory.network.fraction=0.1 \
    -c lakepump.kafka.MysqlCDC2Kafka $jar_path dbInstance=mysql_account serverId=5601-5604 sharding=$1 appName=$appName \
    dbTables=acctdb.acct_item_total_month_[0-9]{6},acctdb.order_info_log,acctdb.payment \
    buckets=100,8,10
}

################### hudi
#  -Dhigh-availability.type=zookeeper \
#  -Dhigh-availability.zookeeper.quorum=hdp-s092.nm.ctdcp.com:2181,hdp-s047.nm.ctdcp.com:2181,hdp-s048.nm.ctdcp.com:2181,hdp-s049.nm.ctdcp.com:2181,hdp-s051.nm.ctdcp.com:2181 \
#  -Dhigh-availability.zookeeper.path.root=/flink-ha/$appName \
#  -Dhigh-availability.cluster-id=/$appName \
#  -Dhigh-availability.storageDir=hdfs://ctyunns/user/ads/flink/ha/recovery \
function run2hudi_crm() {
  local appName="cdc2hudi_mysql_crm_sharding_$1"
  $flink_bin_dir/flink run-application -t yarn-application -Dyarn.provided.lib.dirs=hdfs://ctyunns/user/ads/flink/lib2 \
    -Dsecurity.kerberos.login.use-ticket-cache=false -Dsecurity.kerberos.login.contexts=Client,KafkaClient \
    -Dsecurity.kerberos.login.principal=ads/hdp-tmp007.nm.ctdcp.com@BIGDATA.CHINATELECOM.CN \
    -Dsecurity.kerberos.login.keytab=/etc/security/keytabs/ads.keytab \
    -Dclassloader.check-leaked-classloader=false \
    -Djobmanager.archive.fs.dir=hdfs://ctyunns/flink-history/realjob -Dhistoryserver.archive.fs.dir=hdfs://ctyunns/flink-history/realjob \
    -Dclient.timeout=600s -Dtaskmanager.slot.timeout=300s -Dakka.ask.timeout=300s \
    -Dyarn.application.name=$appName -Dyarn.application.queue=dws \
    -Djobmanager.memory.process.size=8gb -Dtaskmanager.numberOfTaskSlots=4 -Dparallelism.default=4 \
    -Dtaskmanager.memory.process.size=32gb -Dtaskmanager.memory.managed.fraction=0.2 -Dtaskmanager.memory.network.fraction=0.1 \
    -c lakepump.hudi.MysqlCDC2Hudi $jar_path dbInstance=mysql_crm serverId=5611-5614 sharding=$1 appName=$appName \
    dbTables=order.inner_order_attr_his \
    buckets=104
  #  dbTables=cust.prod_spec_inst,cust.prod_spec_inst_attr,cust.prod_spec_res_inst,order.inner_ord_offer_inst_pay_info_his,order.inner_ord_prod_spec_inst_his,order.master_order_attr_his,order.master_order_his,order.ord_prod_spec_inst_his,order.order_attr_his,order.order_item_his,order.order_pay_info_his,order.inner_order_attr_his,order.inner_ord_offer_inst_his,order.inner_order_meta_his,cust.offer_prod_inst_rel,cust.offer_price_plan_inst,cust.account_attr,cust.project_obj_relation \
  #  buckets=23,280,23,20,24,93,13,22,80,44,3,104,24,15,23,17,23,10
  #  minusDbTables=order.order_item_his,order.order_pay_info_his
}

function run2hudi_account() {
  local appName="cdc2hudi_mysql_account_sharding_$1"
  $flink_bin_dir/flink run-application -t yarn-application -Dyarn.provided.lib.dirs=hdfs://ctyunns/user/ads/flink/lib2 \
    -Dsecurity.kerberos.login.use-ticket-cache=false -Dsecurity.kerberos.login.contexts=Client,KafkaClient \
    -Dsecurity.kerberos.login.principal=ads/hdp-tmp007.nm.ctdcp.com@BIGDATA.CHINATELECOM.CN \
    -Dsecurity.kerberos.login.keytab=/etc/security/keytabs/ads.keytab \
    -Dclassloader.check-leaked-classloader=false \
    -Djobmanager.archive.fs.dir=hdfs://ctyunns/flink-history/realjob -Dhistoryserver.archive.fs.dir=hdfs://ctyunns/flink-history/realjob \
    -Dclient.timeout=600s -Dtaskmanager.slot.timeout=300s -Dakka.ask.timeout=300s \
    -Dyarn.application.name=$appName -Dyarn.application.queue=dws \
    -Djobmanager.memory.process.size=4gb -Dtaskmanager.numberOfTaskSlots=4 -Dparallelism.default=4 \
    -Dtaskmanager.memory.process.size=8gb -Dtaskmanager.memory.managed.fraction=0.2 -Dtaskmanager.memory.network.fraction=0.1 \
    -c lakepump.hudi.MysqlCDC2Hudi $jar_path dbInstance=mysql_account serverId=5611-5614 sharding=$1 appName=$appName \
    dbTables=acctdb.payment \
    buckets=10
}

function hudi_compact() {
  local APP_ID=$(get_appid_using_name "compact_hudi_tables")
  if [ -z "$APP_ID" ]; then
    $flink_bin_dir/flink run-application -t yarn-application -Dyarn.provided.lib.dirs=hdfs://ctyunns/user/ads/flink/lib2 \
      -Dsecurity.kerberos.login.use-ticket-cache=false -Dsecurity.kerberos.login.contexts=Client,KafkaClient \
      -Dsecurity.kerberos.login.principal=ads/hdp-tmp007.nm.ctdcp.com@BIGDATA.CHINATELECOM.CN \
      -Dsecurity.kerberos.login.keytab=/etc/security/keytabs/ads.keytab \
      -Dclassloader.check-leaked-classloader=false \
      -Dclient.timeout=600s -Dtaskmanager.slot.timeout=300s -Dakka.ask.timeout=300s \
      -Dyarn.application.name=compact_hudi_tables -Dyarn.application.queue=ads \
      -Djobmanager.memory.process.size=4gb -Dtaskmanager.numberOfTaskSlots=4 -Dparallelism.default=4 \
      -Dtaskmanager.memory.process.size=16gb -Dtaskmanager.memory.managed.fraction=0.1 -Dtaskmanager.memory.network.fraction=0.1 \
      -c lakepump.hudi.TimingCompactor $jar_path \
      dbTables=cust.prod_spec_inst,cust.prod_spec_inst_attr,cust.prod_spec_res_inst,order.inner_ord_offer_inst_pay_info_his,order.inner_ord_prod_spec_inst_his,order.master_order_attr_his,order.master_order_his,order.ord_prod_spec_inst_his,order.order_attr_his,order.order_item_his,order.order_pay_info_his,order.inner_order_attr_his,order.inner_ord_offer_inst_his,order.inner_order_meta_his,cust.offer_prod_inst_rel,cust.offer_price_plan_inst,cust.account_attr,cust.project_obj_relation \
      buckets=23,280,23,20,24,93,13,22,80,44,3,104,24,15,23,17,23,10
  fi
}

#################################
function invoke_function() {
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

if [ -n "$auto_switch" ]; then
  # AUTO >>>>>>>>>>>>>>
  yarn_app_list_path="./yarn_app_list.txt"
  yarn application -list > $yarn_app_list_path
  parts=(0 1 2 3 4 5 6 7)
  for i in "${parts[@]}"; do
    invoke_function "run2kafka_crm $i" "cdc2kafka_mysql_crm_sharding_$i"
    invoke_function "run2kafka_account $i" "cdc2kafka_mysql_account_sharding_$i"
    invoke_function "run2hudi_crm $i" "cdc2hudi_mysql_crm_sharding_$i"
  #  invoke_function "run2hudi_account $i" "cdc2hudi_mysql_account_sharding_$i"
  done
  invoke_function hudi_compact "compact_hudi_tables"
else
  # TODO >>>>>>>>>>>>>>
  my_list=(0 1 2 3 4 5 6 7)
  #my_list=(7)
  for i in "${my_list[@]}"; do
  #  shutdown_and_clean cdc2kafka_mysql_crm_sharding_$i
  #  shutdown_and_clean cdc2kafka_mysql_account_sharding_$i
#    shutdown_and_clean cdc2hudi_mysql_crm_sharding_$i $i
  #  shutdown_and_clean cdc2hudi_mysql_account_sharding_$i $i

  #  run2kafka_crm $i
  #  run2kafka_account $i
    run2hudi_crm $i
  #  run2hudi_account $i
  done
  hudi_compact
fi

#################################
# DEBUG
#$flink_bin_dir/flink run-application -t yarn-application -Dyarn.provided.lib.dirs=hdfs://ctyunns/user/ads/flink/lib2 \
#  -Dsecurity.kerberos.login.use-ticket-cache=false -Dsecurity.kerberos.login.contexts=Client,KafkaClient \
#  -Dsecurity.kerberos.login.principal=ads/hdp-tmp007.nm.ctdcp.com@BIGDATA.CHINATELECOM.CN \
#  -Dsecurity.kerberos.login.keytab=/etc/security/keytabs/ads.keytab \
#  -Dclassloader.check-leaked-classloader=false \
#  -Djobmanager.archive.fs.dir=hdfs://ctyunns/flink-history/realjob -Dhistoryserver.archive.fs.dir=hdfs://ctyunns/flink-history/realjob \
#  -Dclient.timeout=600s -Dtaskmanager.slot.timeout=300s -Dakka.ask.timeout=300s \
#  -Dyarn.application.name=consume_test -Dyarn.application.queue=ads \
#  -Djobmanager.memory.process.size=1gb -Dtaskmanager.numberOfTaskSlots=1 -Dparallelism.default=1 \
#  -Dtaskmanager.memory.process.size=1gb -Dtaskmanager.memory.managed.fraction=0.1 -Dtaskmanager.memory.network.fraction=0.1 \
#  -c lakepump.kafka.TestCDCConsumer $jar_path acctdb.acct_item_total_month_[0-9]{6} 2023-12-11_00:00:00 t_acctdb_acct_item_total_month

