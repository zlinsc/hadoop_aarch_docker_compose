#!/bin/bash
export HADOOP_CLASSPATH=`hadoop classpath`

function run() {
  local appName="cdc2hudi_$1_sharding_$2"
./flink-1.17.0-x/bin/flink run-application -t yarn-application -Dyarn.provided.lib.dirs=hdfs://ctyunns/user/ads/flink/lib2 \
  -Dsecurity.kerberos.login.use-ticket-cache=false -Dsecurity.kerberos.login.contexts=Client,KafkaClient \
  -Dsecurity.kerberos.login.principal=ads/hdp-tmp007.nm.ctdcp.com@BIGDATA.CHINATELECOM.CN \
  -Dsecurity.kerberos.login.keytab=/etc/security/keytabs/ads.keytab \
  -Djobmanager.archive.fs.dir=hdfs://ctyunns/flink-history/realjob -Dhistoryserver.archive.fs.dir=hdfs://ctyunns/flink-history/realjob \
  -Dclient.timeout=600s -Dtaskmanager.slot.timeout=300s -Dakka.ask.timeout=300s \
  -Dyarn.application.name=$appName -Dyarn.application.queue=ads \
  -Djobmanager.memory.process.size=8gb -Dtaskmanager.numberOfTaskSlots=4 -Dparallelism.default=4 \
  -Dtaskmanager.memory.process.size=32gb -Dtaskmanager.memory.managed.fraction=0.2 taskmanager.memory.network.fraction=0.1 \
  -c lakepump.hudi.MysqlCDC2Hudi flink_work-1.2.jar dbInstance=$1 serverId=5601-5604 sharding=$2 appName=$appName \
  dbTables=cust.prod_spec_inst_rel,cust.prod_spec_inst,cust.prod_spec_inst_attr,cust.prod_spec_res_inst,order.inner_ord_offer_inst_pay_info_his,order.inner_ord_prod_spec_inst_his,order.master_order_attr_his,order.master_order_his,order.ord_prod_spec_inst_his,order.order_attr_his,order.order_item_his,order.order_pay_info_his,order.inner_order_attr_his,order.inner_ord_offer_inst_his,order.inner_order_meta_his,cust.offer_prod_inst_rel,cust.offer_price_plan_inst,cust.account_attr,cust.project_obj_relation \
  buckets=4,23,280,23,20,24,93,13,22,80,44,3,104,24,15,23,17,23,10
}
run mysql_crm 0

#function compact() {
#./flink-1.17.0-x/bin/flink run-application -t yarn-application -Dyarn.provided.lib.dirs=hdfs://ctyunns/user/ads/flink/lib2 \
#  -Dsecurity.kerberos.login.use-ticket-cache=false -Dsecurity.kerberos.login.contexts=Client,KafkaClient \
#  -Dsecurity.kerberos.login.principal=ads/hdp-tmp007.nm.ctdcp.com@BIGDATA.CHINATELECOM.CN \
#  -Dsecurity.kerberos.login.keytab=/etc/security/keytabs/ads.keytab \
#  -Dclient.timeout=600s -Dtaskmanager.slot.timeout=300s -Dakka.ask.timeout=300s \
#  -Dyarn.application.name=compact_$1_$2 -Dyarn.application.queue=ads \
#  -Djobmanager.memory.process.size=2gb -Dtaskmanager.numberOfTaskSlots=4 -Dparallelism.default=4 \
#  -Dtaskmanager.memory.process.size=10gb -Dtaskmanager.memory.managed.fraction=0.1 -Dtaskmanager.memory.network.fraction=0.1 \
#  -c org.apache.hudi.sink.compact.HoodieFlinkCompactor ./flink-1.17.0-x/lib/hudi-flink1.17-bundle-mod-0.14.0.jar --path hdfs://ctyunns/user/ads/hudi/$1/$2/
#}
#for ((i = 0; i < 8; i++)); do
#  compact hudi_cust offer_price_plan_inst_$i
#done

function compact() {
./flink-1.17.0-x/bin/flink run-application -t yarn-application -Dyarn.provided.lib.dirs=hdfs://ctyunns/user/ads/flink/lib2 \
  -Dsecurity.kerberos.login.use-ticket-cache=false -Dsecurity.kerberos.login.contexts=Client,KafkaClient \
  -Dsecurity.kerberos.login.principal=ads/hdp-tmp007.nm.ctdcp.com@BIGDATA.CHINATELECOM.CN \
  -Dsecurity.kerberos.login.keytab=/etc/security/keytabs/ads.keytab \
  -Dclient.timeout=600s -Dtaskmanager.slot.timeout=300s -Dakka.ask.timeout=300s \
  -Dyarn.application.name=compact_hudi_tables -Dyarn.application.queue=ads \
  -Djobmanager.memory.process.size=8gb -Dtaskmanager.numberOfTaskSlots=4 -Dparallelism.default=4 \
  -Dtaskmanager.memory.process.size=16gb -Dtaskmanager.memory.managed.fraction=0.1 -Dtaskmanager.memory.network.fraction=0.1 \
  -c lakepump.hudi.TimingCompactor flink_work-1.3.jar \
  dbTables=cust.prod_spec_inst_rel,cust.prod_spec_inst,cust.prod_spec_inst_attr,cust.prod_spec_res_inst,order.inner_ord_offer_inst_pay_info_his,order.inner_ord_prod_spec_inst_his,order.master_order_attr_his,order.master_order_his,order.ord_prod_spec_inst_his,order.order_attr_his,order.order_item_his,order.order_pay_info_his,order.inner_order_attr_his,order.inner_ord_offer_inst_his,order.inner_order_meta_his,cust.offer_prod_inst_rel,cust.offer_price_plan_inst,cust.account_attr,cust.project_obj_relation \
  buckets=4,23,280,23,20,24,93,13,22,80,44,3,104,24,15,23,17,23,10
}
compact