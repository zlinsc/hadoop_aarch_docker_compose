#!/bin/bash

export HADOOP_CLASSPATH=`hadoop classpath`
./flink-1.17.0-x/bin/flink run-application -t yarn-application -Dyarn.provided.lib.dirs=hdfs://ctyunns/user/ads/flink/lib2 \
  -Dsecurity.kerberos.login.use-ticket-cache=false -Dsecurity.kerberos.login.contexts=Client,KafkaClient \
  -Dsecurity.kerberos.login.principal=ads/hdp-tmp007.nm.ctdcp.com@BIGDATA.CHINATELECOM.CN \
  -Dsecurity.kerberos.login.keytab=/etc/security/keytabs/ads.keytab \
  -Djobmanager.archive.fs.dir=hdfs://ctyunns/flink-history/realjob -Dhistoryserver.archive.fs.dir=hdfs://ctyunns/flink-history/realjob \
  -Dclient.timeout=600s -Dtaskmanager.slot.timeout=300s -Dakka.ask.timeout=300s \
  -Dyarn.application.name=htest -Dyarn.application.queue=ads \
  -Djobmanager.memory.process.size=4gb -Dtaskmanager.numberOfTaskSlots=4 -Dparallelism.default=4 \
  -Dtaskmanager.memory.process.size=16gb -Dtaskmanager.memory.managed.fraction=0.3 \
  -c MysqlCDC2Hudi flink_work-1.2.jar label=mysql_crm serverId=5601-5604 sharding=0 dbTables=cust.offer_price_plan_inst,cust.prod_spec_inst_rel

