#!/bin/bash
export HADOOP_CLASSPATH=`hadoop classpath`
yarn_resource_web="flink-cdc-003.nm.ctdcp.com:8088"

################### shutdown job
function backup_jobid_cache() {
  local APP_NAME=$1
  hadoop fs -get -f hdfs://ctyunns/user/ads/flink/jobid_cache/$APP_NAME
}
for ((i = 0; i < 8; i++)); do
#  backup_jobid_cache cdc2kafka_mysql_crm_sharding_$i
  backup_jobid_cache cdc2hudi_mysql_crm_sharding_$i
done

function get_appid_using_name() {
  local APP_NAME=$1
  for ((i = 0; i < 1; i++)); do
    local APP_ID=$(yarn application -list | grep "$APP_NAME" | awk '{print $1}')
    echo "$APP_ID"
  done
}

function shutdown() {
  local APP_NAME=$1
  local APP_ID=$(get_appid_using_name "$APP_NAME")
  if [ -n "$APP_ID" ]; then
    local JOB_ID=$(hadoop fs -cat hdfs://ctyunns/user/ads/flink/jobid_cache/$APP_NAME)
    #http://flink-cdc-003.nm.ctdcp.com:8088/proxy/application_1701850654341_0001/jobs/dd6b63cbdbc62acb90ca1582c20446b3/yarn-cancel
    local url="http://$yarn_resource_web/proxy/$APP_ID/jobs/$JOB_ID/yarn-cancel"
    echo $url
    curl -s $url
  fi
}
for ((i = 0; i < 8; i++)); do
#  shutdown cdc2kafka_mysql_crm_sharding_$i
  shutdown cdc2hudi_mysql_crm_sharding_$i
done

################### kafka
function run2kafka() {
  local appName="cdc2kafka_$1_sharding_$2"
./flink-1.17.0-x/bin/flink run-application -t yarn-application -Dyarn.provided.lib.dirs=hdfs://ctyunns/user/ads/flink/lib2 \
  -Dsecurity.kerberos.login.use-ticket-cache=false -Dsecurity.kerberos.login.contexts=Client,KafkaClient \
  -Dsecurity.kerberos.login.principal=ads/hdp-tmp007.nm.ctdcp.com@BIGDATA.CHINATELECOM.CN \
  -Dsecurity.kerberos.login.keytab=/etc/security/keytabs/ads.keytab \
  -Djobmanager.archive.fs.dir=hdfs://ctyunns/flink-history/realjob -Dhistoryserver.archive.fs.dir=hdfs://ctyunns/flink-history/realjob \
  -Dclient.timeout=600s -Dtaskmanager.slot.timeout=300s -Dakka.ask.timeout=300s \
  -Dyarn.application.name=$appName -Dyarn.application.queue=$3 \
  -Djobmanager.memory.process.size=8gb -Dtaskmanager.numberOfTaskSlots=4 -Dparallelism.default=4 \
  -Dtaskmanager.memory.process.size=16gb -Dtaskmanager.memory.managed.fraction=0.2 -Dtaskmanager.memory.network.fraction=0.1 \
  -c lakepump.kafka.MysqlCDC2Kafka flink_work-1.3.jar dbInstance=$1 serverId=5601-5604 sharding=$2 appName=$appName \
  dbTables=order.inner_ord_offer_inst_pay_info,order.master_order,order.master_order_his \
  buckets=1,1,13
}
run2kafka mysql_crm 0 ads
run2kafka mysql_crm 1 ads
run2kafka mysql_crm 2 ads
run2kafka mysql_crm 3 ads
run2kafka mysql_crm 4 ads
run2kafka mysql_crm 5 ads
run2kafka mysql_crm 6 ads
run2kafka mysql_crm 7 ads

################### hudi
function run2hudi() {
  local appName="cdc2hudi_$1_sharding_$2"
./flink-1.17.0-x/bin/flink run-application -t yarn-application -Dyarn.provided.lib.dirs=hdfs://ctyunns/user/ads/flink/lib2 \
  -Dsecurity.kerberos.login.use-ticket-cache=false -Dsecurity.kerberos.login.contexts=Client,KafkaClient \
  -Dsecurity.kerberos.login.principal=ads/hdp-tmp007.nm.ctdcp.com@BIGDATA.CHINATELECOM.CN \
  -Dsecurity.kerberos.login.keytab=/etc/security/keytabs/ads.keytab \
  -Djobmanager.archive.fs.dir=hdfs://ctyunns/flink-history/realjob -Dhistoryserver.archive.fs.dir=hdfs://ctyunns/flink-history/realjob \
  -Dclient.timeout=600s -Dtaskmanager.slot.timeout=300s -Dakka.ask.timeout=300s \
  -Dyarn.application.name=$appName -Dyarn.application.queue=$3 \
  -Djobmanager.memory.process.size=8gb -Dtaskmanager.numberOfTaskSlots=4 -Dparallelism.default=4 \
  -Dtaskmanager.memory.process.size=32gb -Dtaskmanager.memory.managed.fraction=0.2 -Dtaskmanager.memory.network.fraction=0.1 \
  -c lakepump.hudi.MysqlCDC2Hudi flink_work-1.3.jar dbInstance=$1 serverId=5611-5614 sharding=$2 appName=$appName \
  dbTables=cust.prod_spec_inst,cust.prod_spec_inst_attr,cust.prod_spec_res_inst,order.inner_ord_offer_inst_pay_info_his,order.inner_ord_prod_spec_inst_his,order.master_order_attr_his,order.master_order_his,order.ord_prod_spec_inst_his,order.order_attr_his,order.order_item_his,order.order_pay_info_his,order.inner_order_attr_his,order.inner_ord_offer_inst_his,order.inner_order_meta_his,cust.offer_prod_inst_rel,cust.offer_price_plan_inst,cust.account_attr,cust.project_obj_relation \
  buckets=23,280,23,20,24,93,13,22,80,44,3,104,24,15,23,17,23,10
}
run2hudi mysql_crm 0 dws
run2hudi mysql_crm 1 dws
run2hudi mysql_crm 2 dws
run2hudi mysql_crm 3 dws
run2hudi mysql_crm 4 dws
run2hudi mysql_crm 5 dws
run2hudi mysql_crm 6 dws
run2hudi mysql_crm 7 dws

function compact() {
./flink-1.17.0-x/bin/flink run-application -t yarn-application -Dyarn.provided.lib.dirs=hdfs://ctyunns/user/ads/flink/lib2 \
  -Dsecurity.kerberos.login.use-ticket-cache=false -Dsecurity.kerberos.login.contexts=Client,KafkaClient \
  -Dsecurity.kerberos.login.principal=ads/hdp-tmp007.nm.ctdcp.com@BIGDATA.CHINATELECOM.CN \
  -Dsecurity.kerberos.login.keytab=/etc/security/keytabs/ads.keytab \
  -Dclient.timeout=600s -Dtaskmanager.slot.timeout=300s -Dakka.ask.timeout=300s \
  -Dyarn.application.name=compact_hudi_tables -Dyarn.application.queue=ads \
  -Djobmanager.memory.process.size=4gb -Dtaskmanager.numberOfTaskSlots=4 -Dparallelism.default=4 \
  -Dtaskmanager.memory.process.size=16gb -Dtaskmanager.memory.managed.fraction=0.1 -Dtaskmanager.memory.network.fraction=0.1 \
  -c lakepump.hudi.TimingCompactor flink_work-1.3.jar \
  dbTables=cust.prod_spec_inst,cust.prod_spec_inst_attr,cust.prod_spec_res_inst,order.inner_ord_offer_inst_pay_info_his,order.inner_ord_prod_spec_inst_his,order.master_order_attr_his,order.master_order_his,order.ord_prod_spec_inst_his,order.order_attr_his,order.order_item_his,order.order_pay_info_his,order.inner_order_attr_his,order.inner_ord_offer_inst_his,order.inner_order_meta_his,cust.offer_prod_inst_rel,cust.offer_price_plan_inst,cust.account_attr,cust.project_obj_relation \
  buckets=23,280,23,20,24,93,13,22,80,44,3,104,24,15,23,17,23,10
}
compact